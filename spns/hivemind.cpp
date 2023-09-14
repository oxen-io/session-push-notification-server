#include "hivemind.hpp"

#include <fmt/chrono.h>
#include <oxenc/base32z.h>
#include <oxenc/bt_producer.h>
#include <oxenc/bt_serialize.h>
#include <oxenmq/batch.h>
#include <spdlog/common.h>
#include <systemd/sd-daemon.h>

#include <chrono>
#include <nlohmann/json.hpp>
#include <oxen/log.hpp>
#include <set>

#include "blake2b.hpp"
#include "hive/signature.hpp"

namespace spns {

namespace log = oxen::log;
static auto cat = log::Cat("hivemind");
static auto omq_cat = log::Cat("oxenmq");
static auto stats = log::Cat("stats");

static void omq_log(oxenmq::LogLevel level, const char* file, int line, std::string msg) {
    // We bump the oxenmq log levels down one severity because oxenmq logging is probably less
    // relevant.
    if (level == oxenmq::LogLevel::trace)
        return;
    auto lvl = level == oxenmq::LogLevel::fatal ? log::Level::err
             : level == oxenmq::LogLevel::error ? log::Level::warn
             : level == oxenmq::LogLevel::warn  ? log::Level::info
             : level == oxenmq::LogLevel::info  ? log::Level::debug
                                                : log::Level::trace;
    omq_cat->log(spdlog::source_loc{file, line, ""}, lvl, "{}", msg);
}

HiveMind::HiveMind(Config conf_in) :
        config{std::move(conf_in)},
        pool_{config.pg_connect},
        omq_{std::string{config.pubkey.sv()},
             std::string{config.privkey.sv()},
             false,
             nullptr,
             omq_log} {

    fiddle_rlimit_nofile();

    sd_notify(0, "STATUS=Initializing OxenMQ");

    // Ignore debugging and below; get everything else and let our logger filter it
    omq_.log_level(oxenmq::LogLevel::info);

    while (omq_push_.size() < config.omq_push_instances) {
        auto& o = omq_push_.emplace_back(
                std::string{config.pubkey.sv()},
                std::string{config.privkey.sv()},
                false,
                nullptr,
                omq_log);
        o.MAX_SOCKETS = 50000;
        o.MAX_MSG_SIZE = 10 * 1024 * 1024;
        o.EPHEMERAL_ROUTING_ID = false;
        o.log_level(oxenmq::LogLevel::info);
        // Since we're splitting the load, we reduce number of workers per push server to
        // ceil(instances/N) + 1 (the +1 because the load is probably not perfectly evenly
        // distributed).
        o.set_general_threads(
                1 + (std::thread::hardware_concurrency() + config.omq_push_instances - 1) /
                            config.omq_push_instances);
    }
    omq_push_next_ = omq_push_.begin();

    if (omq_push_.empty()) {
        // the main omq_ is dealing with push conns and notifications so increase limits
        omq_.MAX_SOCKETS = 50000;
        omq_.MAX_MSG_SIZE = 10 * 1024 * 1024;
        omq_.EPHEMERAL_ROUTING_ID = false;

        // We always need to ensure we have some batch threads available because for swarm updates
        // we keep a lock held during the batching and need to ensure that there will always be some
        // workers available, even if a couple workers lock waiting on that lock.
        omq_.set_batch_threads(std::max<int>(4, std::thread::hardware_concurrency() / 2));
    } else {
        // When in multi-instance mode the main worker can get by with fewer threads
        omq_.set_general_threads(std::max<int>(4, std::thread::hardware_concurrency() / 4));
        omq_.set_batch_threads(std::max<int>(4, std::thread::hardware_concurrency() / 4));
    }

    // We listen on a local socket for connections from other local services (web frontend,
    // notification services).
    omq_.listen_plain(
            config.hivemind_sock, [](std::string_view addr, std::string_view /*pk*/, bool /*sn*/) {
                log::info(cat, "Incoming local sock connection from {}", addr);
                return oxenmq::AuthLevel::admin;
            });
    log::info(cat, "Listening for local connections on {}", config.hivemind_sock);

    if (config.hivemind_curve) {
        auto allow_curve_conn = [this](std::string_view addr, std::string_view pk, bool /*sn*/) {
            bool is_admin = false;
            for (const auto& admin : config.hivemind_curve_admin) {
                if (admin.sv() == pk) {
                    is_admin = true;
                    break;
                }
            }
            log::info(cat, "Incoming {} connection from {}", is_admin ? "admin" : "public", addr);
            return is_admin ? oxenmq::AuthLevel::admin : oxenmq::AuthLevel::none;
        };
        omq_.listen_curve(*config.hivemind_curve, std::move(allow_curve_conn));

        std::string log_addr = *config.hivemind_curve;
        if (starts_with(log_addr, "tcp://"))
            log_addr = "curve://{}/{}"_format(
                    log_addr.substr(6), oxenc::to_base32z(omq_.get_pubkey()));
        log::info(cat, "Listening for incoming connections on {}", log_addr);
    }

    // Keep a fairly large queue so that we can handle a sudden influx of notifications; if using
    // multiple instances, use smaller individual queues but with a slightly higher overall queue.
    int notify_queue_size = omq_push_.size() <= 1 ? 4000 : (6000 / omq_push_.size());

    // Invoked by our oxend to notify of a new block:
    omq_.add_category("notify", oxenmq::AuthLevel::basic, /*reserved_threads=*/0, notify_queue_size)
            .add_command("block", ExcWrapper{*this, &HiveMind::on_new_block, "on_new_block"});

    if (omq_push_.empty())
        omq_.add_request_command(
                "notify",
                "message",
                ExcWrapper{*this, &HiveMind::on_message_notification, "on_message_notification"});
    else
        for (auto& push : omq_push_)
            push.add_category(
                        "notify",
                        oxenmq::AuthLevel::basic,
                        /*reserved_threads=*/0,
                        notify_queue_size)
                    .add_command(
                            "message",
                            ExcWrapper{
                                    *this,
                                    &HiveMind::on_message_notification,
                                    "on_message_notification"});

    omq_.add_category("push", oxenmq::AuthLevel::none)

            // Adds/updates a subscription.  This is called from the HTTP process to pass along an
            // incoming (re)subscription.  The request must be json such as:
            //
            // {
            //     "pubkey": "05123...",
            //     "session_ed25519": "abc123...",
            //     "subkey_tag": "def789...",
            //     "namespaces": [-400,0,1,2,17],
            //     "data": true,
            //     "sig_ts": 1677520760,
            //     "signature": "f8efdd120007...",
            //     "service": "apns",
            //     "service_info": { ... },
            //     "enc_key": "abcdef..." (32 bytes: 64 hex or 43 base64).
            // }
            //
            // The `service_info` argument is passed along to the underlying notification provider
            // and must contain whatever info is required to send notifications to the device:
            // typically some device ID, and possibly other data.  It is specific to each
            // notification provider.
            //
            // The reply is JSON; an error looks like:
            //
            //     { "error": 123, "message": "Something getting wrong!" }
            //
            // where "error" is one of the hive/subscription.hpp SUBSCRIBE enum values.
            //
            // On a successful subscription you get back one of:
            //
            //     { "success": true, "added": true, "message": "Subscription successful" }
            //
            //     { "success": true, "updated": true, "message": "Resubscription successful" }
            //
            // Note that the "message" strings are subject to change and should not be relied on
            // programmatically; instead rely on the "error" or "success" values.
            .add_request_command(
                    "subscribe", ExcWrapper{*this, &HiveMind::on_subscribe, "on_subscribe", true})

            .add_request_command(
                    "unsubscribe",
                    ExcWrapper{*this, &HiveMind::on_unsubscribe, "on_unsubscribe", true})

            // end of "push." commands
            ;

    // Commands for local services to talk to us:
    omq_.add_category("admin", oxenmq::AuthLevel::admin)

            // Registers a notification service.  This gets called with a single argument containing
            // the service name(s) (e.g. "apns", "firebase") that should be pushed to this
            // connection when notifications or subscriptions arrive.  (If a single connection
            // provides multiple services it should invoke this endpoint multiple times).
            //
            // The invoking OMQ connection must accept two commands:
            //
            // `notifier.validate` request command.  This is called on an incoming subscription or
            // unsubscription to validate and parse it.  It is passed a two-part message: the
            // service name (e.g. b"apns") that the client requested, and the JSON registration data
            // as supplied by the client.  The return is one of:
            //
            // - [b'0', b'unique service id', b'supplemental data']  (acceptable registration)
            // - [b'0', b'unique service id']   (acceptable, with no supplemental data)
            // - [b'4', b'Error string']  (non-zero code: code and error message returned to the
            //   client)
            //
            // where the unique service id must be a utf8-encoded string that is at least 32
            // characters long and unique for the device/app in question (if the same service id for
            // the same service already exists, the registration is replaced; otherwise it is a new
            // registration). The supplemental data will be stored and passed along when
            // notifications are provided to the following command.  The remote should *not* store
            // local state associated with the registration: instead everything is meant to be
            // stored by the hivemind caller and then passed back in (via the following endpoint).
            //
            // `notifier.push` is a (non-request) command.  This is called when a user is to be
            // notified of an incoming message.  It is a single-part, bencoded dict containing:
            //
            // - '' -- the service name, e.g. b"apns"
            // - '&' -- the unique service id (as was provided by the validate endpoint).
            // - '!' -- supplemental service data, if the validate request returned any; omitted
            //   otherwise.
            // - '^' -- the xchacha20-poly1305encryption key the user gave when registering for
            //   notifications with which the notification payload should be encrypted.
            // - '#' -- the message hash from storage server.
            // - '@' -- the account ID (Session ID or closed group ID) to which the message was sent
            //   (33 bytes).
            // - 'n' -- the swarm namespace to which the message was deposited (-32768 to 32767).
            // - '~' -- the encrypted message data; this field will not be present if the
            //   registration did not request data.
            .add_command(
                    "register_service",
                    ExcWrapper{*this, &HiveMind::on_reg_service, "on_reg_service"})

            // Called periodically to notify us of notifier stats (notifications, failures, etc.)
            .add_command(
                    "service_stats",
                    ExcWrapper{*this, &HiveMind::on_service_stats, "on_service_stats"})

            // Retrieves current statistics
            .add_request_command(
                    "get_stats", ExcWrapper{*this, &HiveMind::on_get_stats, "on_get_stats"})

            // end of "admin." commands
            ;

    sd_notify(0, "STATUS=Cleaning database");
    db_cleanup();
    sd_notify(0, "STATUS=Loading existing subscriptions");
    load_saved_subscriptions();

    {
        std::lock_guard lock{mutex_};

        sd_notify(0, "STATUS=Starting OxenMQ");
        log::info(cat, "Starting OxenMQ");
        omq_.start();
        for (auto& o : omq_push_)
            o.start();

        log::info(cat, "Started OxenMQ");

        sd_notify(0, "STATUS=Connecting to oxend");
        log::info(cat, "Connecting to oxend @ {}", config.oxend_rpc.full_address());

        std::promise<void> prom;
        oxend_ = omq_.connect_remote(
                config.oxend_rpc,
                [&prom](auto) { prom.set_value(); },
                [&prom](auto, std::string_view err) {
                    try {
                        throw std::runtime_error{"oxend connection failed: " + std::string{err}};
                    } catch (...) {
                        prom.set_exception(std::current_exception());
                    }
                },
                oxenmq::AuthLevel::basic);

        log::info(cat, "Waiting for oxend connection...");
        prom.get_future().get();

        prom = {};
        omq_.request(oxend_, "ping.ping", [&prom](bool success, std::vector<std::string> data) {
            if (success)
                prom.set_value();
            else
                try {
                    std::string err = "oxend failed to respond to ping:";
                    if (data.empty())
                        data.push_back("(unknown)");
                    for (auto& m : data) {
                        err += ' ';
                        err += m;
                    }
                    throw std::runtime_error{err};
                } catch (...) {
                    prom.set_exception(std::current_exception());
                }
        });

        prom.get_future().get();
        log::info(cat, "Connected to oxend");

        sd_notify(0, "STATUS=Waiting for notifiers");

        if (config.notifier_wait > 0s) {
            // Wait for notification servers that start up before or alongside us to connect:
            auto wait_until = steady_clock::now() + config.notifier_wait;
            log::info(
                    cat,
                    "Waiting for notifiers to register (max {})",
                    wait_until - steady_clock::now());
            while (!notifier_startup_done(wait_until)) {
                mutex_.unlock();
                std::this_thread::sleep_for(25ms);
                mutex_.lock();
            }
            log::info(cat, "Done waiting for notifiers; {} registered", services_.size());
        }
    }

    // Set our ready flag, and process any requests that accumulated while we were starting up.
    set_ready();

    refresh_sns();

    omq_.add_timer([this] { db_cleanup(); }, 30s);
    // This is for operations that can be high latency, like re-subscriptions, clearing expiries,
    // etc.:
    omq_.add_timer([this] { subs_slow(); }, config.subs_interval);

    // For updating systemd Status line
    omq_.add_timer([this] { log_stats(); }, 15s);

    // This one is much more frequent: it handles any immediate subscription duties (e.g. to
    // deal with a new subscriber we just added):
    omq_.add_timer([this] { subs_fast(); }, 100ms);

    log::info(cat, "Startup complete");
}

bool HiveMind::notifier_startup_done(const steady_time& wait_until) {
    // NB: lock is held by the caller

    // If we were told which notifiers to wait for then check to see if they are all present, and if
    // so return early:
    std::vector<std::string_view> missing;
    if (!config.notifiers_expected.empty()) {
        for (const auto& service : config.notifiers_expected) {
            if (!services_.count(service))
                missing.emplace_back(service);
        }
        if (missing.empty()) {
            log::info(cat, "All configured notifiers have registered");
            return true;
        }
    }

    // Otherwise we keep waiting until wait_until
    bool done_waiting = steady_clock::now() > wait_until;

    if (done_waiting && !config.notifiers_expected.empty())
        log::warning(
                cat,
                "Notifier startup timeout reached; did not receive registrations for: {}",
                "{}"_format(fmt::join(missing, ", ")));

    return done_waiting;
}

void HiveMind::set_ready() {
    // Set `ready` with this main mutex held (even though it is atomic!) so that we can be sure that
    // nothing gets added to `deferred_` between the time we set it, and draining it below.
    // (defer_request handles the race: if it gets the lock and `ready` has been flipped to true, it
    // notices and calls right away instead of adding to deferred_).
    {
        std::lock_guard lock{deferred_mutex_};
        ready = true;
    }
    log_stats("READY=1");

    while (!deferred_.empty()) {
        std::move(deferred_.front())();
        deferred_.pop_front();
    }
}
void HiveMind::defer_request(oxenmq::Message&& m, ExcWrapper& callback) {
    {
        std::lock_guard lock{deferred_mutex_};
        if (!ready) {
            deferred_.emplace_back(std::move(m), callback);
            return;
        }
    }
    // Must have flipped between the check and now, so don't actually defer it
    callback(m);
}
DeferredRequest::DeferredRequest(oxenmq::Message&& m, ExcWrapper& callback) :
        message{m.oxenmq, std::move(m.conn), std::move(m.access), std::move(m.remote)},
        callback{callback} {
    data.reserve(m.data.size());
    for (const auto& d : m.data)
        message.data.emplace_back(data.emplace_back(d));
}
void ExcWrapper::operator()(oxenmq::Message& m) {
    try {
        (hivemind.*meth)(m);
    } catch (const startup_request_defer&) {
        hivemind.defer_request(std::move(m), *this);
    } catch (const std::exception& e) {
        log::error(cat, "Exception in HiveMind::{}: {}", meth_name, e.what());
        if (is_json_request) {
            m.send_reply(nlohmann::json{
                    {"error", static_cast<int>(hive::SUBSCRIBE::INTERNAL_ERROR)},
                    {"message", "An internal error occurred while processing your request"}}
                                 .dump());
        }
    }
}

void HiveMind::on_reg_service(oxenmq::Message& m) {
    if (m.data.size() != 1) {
        log::error(cat, "{}-part data, expected 1", m.data.size());
        return;
    }
    std::string service{m.data[0]};
    if (service.empty()) {
        log::error(cat, "service registration used illegal empty service name");
        return;
    }
    if (service.size() > 32) {
        log::error(cat, "service name too long ({})", service.size());
        return;
    }

    bool added = false, replaced = false;
    {
        std::lock_guard lock{mutex_};
        auto [it, ins] = services_.emplace(service, m.conn);
        if (ins)
            added = true;
        else if (m.conn != it->second) {
            it->second = m.conn;
            replaced = true;
        }
    }

    if (added)
        log::info(cat, "'{}' notification service registered", service);
    else if (replaced)
        log::info(cat, "'{}' notification service reconnected/reregistered", service);
    else
        log::trace(cat, "'{}' notification service confirmed (already registered)", service);
}

static void set_stat(
        pqxx::work& tx, std::string_view service, std::string_view name, std::string_view val) {
    tx.exec_params0(
            R"(
INSERT INTO service_stats (service, name, val_str) VALUES ($1, $2, $3)
ON CONFLICT (service, name) DO UPDATE
    SET val_str = EXCLUDED.val_str, val_int = NULL)",
            service,
            name,
            val);
}
static void set_stat(pqxx::work& tx, std::string_view service, std::string_view name, int64_t val) {
    tx.exec_params0(
            R"(
INSERT INTO service_stats (service, name, val_int) VALUES ($1, $2, $3)
ON CONFLICT (service, name) DO UPDATE
    SET val_str = NULL, val_int = EXCLUDED.val_int)",
            service,
            name,
            val);
}
static void increment_stat(
        pqxx::work& tx, std::string_view service, std::string_view name, int64_t incr) {
    tx.exec_params0(
            R"(
INSERT INTO service_stats (service, name, val_int) VALUES ($1, $2, $3)
ON CONFLICT (service, name) DO UPDATE
    SET val_str = NULL, val_int = COALESCE(service_stats.val_int, 0) + EXCLUDED.val_int)",
            service,
            name,
            incr);
}

void HiveMind::on_message_notification(oxenmq::Message& m) {
    if (m.data.size() != 1) {
        log::warning(
                cat,
                "Unexpected message notification: {}-part data, expected 1-part",
                m.data.size());
        return;
    }

    oxenc::bt_dict_consumer dict{m.data[0]};

    // Parse oxen-storage-server notification:
    if (!dict.skip_until("@")) {
        log::warning(cat, "Unexpected notification: missing account (@)");
        return;
    }
    auto account_str = dict.consume_string_view();
    AccountID account;
    if (account_str.size() != account.SIZE) {
        log::warning(cat, "Unexpected notification: wrong account size (@)");
        return;
    }
    std::memcpy(account.data(), account_str.data(), account.size());

    if (!dict.skip_until("h")) {
        log::warning(cat, "Unexpected notification: missing msg hash (h)");
        return;
    }
    auto hash = dict.consume_string_view();
    if (bool too_small = hash.size() < MSG_HASH_MIN_SIZE;
        too_small || hash.size() > MSG_HASH_MAX_SIZE) {
        log::warning(cat, "Unexpected notification: msg hash too small");
        return;
    }

    if (!dict.skip_until("n")) {
        log::warning(cat, "Unexpected notification: missing namespace (n)");
        return;
    }
    auto ns = dict.consume_integer<int16_t>();

    if (!dict.skip_until("t")) {
        log::warning(cat, "Unexpected notification: missing message timestamp (t)");
        return;
    }
    auto timestamp_ms = dict.consume_integer<int64_t>();

    if (!dict.skip_until("z")) {
        log::warning(cat, "Unexpected notification: missing message expiry (z)");
        return;
    }
    auto expiry_ms = dict.consume_integer<int64_t>();

    std::optional<std::string_view> maybe_data;
    if (dict.skip_until("~"))
        maybe_data = dict.consume_string_view();

    log::trace(
            cat,
            "Got a notification for {}, msg hash {}, namespace {}, timestamp {}, exp {}, data {}B",
            account.hex(),
            hash,
            ns,
            timestamp_ms,
            expiry_ms,
            maybe_data ? fmt::to_string(maybe_data->size()) : "(N/A)");

    // [(want_data, enc_key, service, svcid, svcdata), ...]
    std::vector<std::tuple<bool, EncKey, std::string, std::string, std::optional<bstring>>>
            notifies;
    std::vector<Blake2B_32> filter_vals;

    auto conn = pool_.get();
    pqxx::work tx{conn};

    auto result = tx.exec_params(
            R"(
SELECT want_data, enc_key, service, svcid, svcdata FROM subscriptions
WHERE account = $1
    AND EXISTS(SELECT 1 FROM sub_namespaces WHERE subscription = id AND namespace = $2))",
            account,
            ns);
    notifies.reserve(result.size());
    filter_vals.reserve(result.size());
    for (auto row : result) {
        row.to(notifies.emplace_back());
        auto& [_wd, _ek, service, svcid, _sd] = notifies.back();
        filter_vals.push_back(blake2b(service, svcid, hash));
    }

    if (notifies.empty()) {
        log::debug(cat, "No active notifications match, ignoring notification");
        tx.commit();
        return;
    }

    size_t notify_count = 0;
    {
        std::lock_guard lock{mutex_};

        if (auto now = steady_clock::now(); now >= filter_rotate_time_) {
            filter_rotate_ = std::move(filter_);
            filter_.clear();
            filter_rotate_time_ = now + config.filter_lifetime;
        }

        assert(filter_vals.size() == notifies.size());
        auto filter_it = filter_vals.begin();
        std::string buf;
        for (auto& [want_data, enc_key, service, svcid, svcdata] : notifies) {
            auto& filt_hash = *filter_it++;

            if (filter_rotate_.count(filt_hash) || !filter_.insert(filt_hash).second) {
                log::debug(cat, "Ignoring duplicate notification");
                continue;
            } else {
                log::trace(cat, "Not filtered: {}", filt_hash.hex());
            }

            oxenmq::ConnectionID conn;
            if (auto it = services_.find(service); it != services_.end())
                conn = it->second;
            else {
                log::warning(
                        cat, "Notification depends on unregistered service {}, ignoring", service);
                continue;
            }

            // We overestimate a little here (e.g. allowing for 20 spaces for string lengths)
            // because a few extra bytes of allocation doesn't really matter.
            size_t size_needed = 2 + 35 +                 // 0: 32:service (or shorter)
                                 3 + 21 + svcid.size() +  // 1:& N:svcid
                                 3 + 35 +                 // 1:^ 32:enckey
                                 3 + 21 + hash.size() +   // 1:# N:hash
                                 3 + 36 +                 // 1:@ 33:account
                                 3 + 8 +                  // 1:n i-32768e
                                 (svcdata ? 3 + 21 + svcdata->size() : 0) +
                                 (want_data && maybe_data ? 3 + 21 + maybe_data->size() : 0);

            if (buf.size() < size_needed)
                buf.resize(size_needed);

            oxenc::bt_dict_producer dict{buf.data(), buf.data() + buf.size()};

            try {
                // NB: ascii sorted keys
                dict.append("", service);
                if (svcdata)
                    dict.append("!", as_sv(*svcdata));
                dict.append("#", hash);
                dict.append("&", svcid);
                dict.append("@", account.sv());
                dict.append("^", enc_key.sv());
                dict.append("n", ns);
                if (want_data && maybe_data)
                    dict.append("~", *maybe_data);
            } catch (const std::exception& e) {
                log::critical(cat, "failed to build notifier message: bad size estimation?");
                continue;
            }

            log::debug(cat, "Sending push via {} notifier", service);
            omq_.send(conn, "notifier.push", dict.view());
            notify_count++;
        }
    }

    increment_stat(tx, "", "notifications", notify_count);
    tx.commit();
}

/// Called from a notifier service periodically to report statistics.
///
/// This should be called with a two-part message: the first part is the service name (e.g.
/// 'apns'); the second part is a bt-encoded dict with content such as:
///
/// {
///     '+notifies': 12,
///     '+failures': 0,
///     'other': 123
/// }
///
/// Integer values using a key beginning with a + will have the local stat (without the +) for
/// the notifier modified by the given integer value; otherwise values will be replaced.  Only
/// integer and string values are permitted (+keys only allow integers).
void HiveMind::on_service_stats(oxenmq::Message& m) {
    if (m.data.size() != 2) {
        log::warning(cat, "Invalid admin.service_stats call: expected 2-part message");
        return;
    }

    auto service = m.data[0];
    if (service.empty()) {
        log::warning(cat, "service status received illegal empty service name");
        return;
    }

    try {
        auto conn = pool_.get();
        pqxx::work tx{conn};
        oxenc::bt_dict_consumer dict{m.data[1]};

        set_stat(tx, "", "last.{}"_format(service), unix_timestamp());
        while (dict) {
            auto key = dict.key();
            if (key.substr(0, 1) == "+") {
                key.remove_prefix(1);
                increment_stat(tx, service, key, dict.consume_integer<int64_t>());
            } else if (dict.is_integer()) {
                set_stat(tx, service, key, dict.consume_integer<int64_t>());
            } else if (dict.is_string()) {
                set_stat(tx, service, key, dict.consume_string_view());
            } else {
                throw std::invalid_argument{
                        "Invalid service status: values must be string or int!"};
            }
        }

        tx.commit();
    } catch (const oxenc::bt_deserialize_invalid_type&) {
        log::warning(cat, "invalid service data: expected int or string data");
    } catch (const std::exception& e) {
        log::warning(cat, "invalid service data: {}", e.what());
    }
}

nlohmann::json HiveMind::get_stats_json() {
    auto result = nlohmann::json{};

    {
        auto conn = pool_.get();
        pqxx::work tx{conn};

        for (auto& [service, name, s, i] :
             tx.query<std::string, std::string, std::optional<std::string>, std::optional<int64_t>>(
                     R"(SELECT service, name, val_str, val_int FROM service_stats)")) {
            if (service == "") {
                if (s)
                    result[name] = std::move(*s);
                else {
                    result[name] = *i;
                    if (starts_with(name, "last."))
                        result["alive." + name.substr(5)] =
                                *i > unix_timestamp(system_clock::now() - 1min);
                }
            } else {
                if (s)
                    result["notifier"][service][name] = std::move(*s);
                else
                    result["notifier"][service][name] = *i;
            }
        }

        int64_t total = 0;
        for (const auto& [service, count] : tx.query<std::string, int64_t>(
                     R"(SELECT service, COUNT(*) FROM subscriptions GROUP BY service)")) {
            result["subscriptions"][service] = count;
            total += count;
        }
        result["subscriptions"]["total"] = total;

        tx.commit();
    }

    {
        std::lock_guard lock{mutex_};
        size_t n_conns = 0;
        for (auto& sn : sns_)
            n_conns += sn.second->connected();

        result["block_hash"] = last_block_.first;
        result["block_height"] = last_block_.second;
        result["swarms"] = swarms_.size();
        result["snodes"] = sns_.size();
        result["accounts_monitored"] = subscribers_.size();
        result["connections"] = n_conns;
        result["pending_connections"] = pending_connects_.load();
        result["uptime"] =
                std::chrono::duration<double>(system_clock::now() - startup_time).count();
    }
    return result;
}

void HiveMind::on_get_stats(oxenmq::Message& m) {
    m.send_reply(get_stats_json().dump());
}

void HiveMind::log_stats(std::string_view pre_cmd) {
    auto s = get_stats_json();

    std::list<std::string> notifiers;
    for (auto& [k, v] : s.items())
        if (starts_with(k, "last."))
            if (auto t = v.get<int64_t>(); t >= unix_timestamp(startup_time) &&
                                           t >= unix_timestamp(system_clock::now() - 1min))
                notifiers.push_back(k.substr(5));

    int64_t total_notifies = 0;
    for (auto& [service, data] : s["notifier"].items())
        if (auto it = data.find("notifies"); it != data.end())
            total_notifies += it->get<int64_t>();

    auto stat_line = fmt::format(
            "SN conns: {}/{} ({} pending); Height: {}; Accts/Subs: {}/{}; svcs: {}; notifies: {}",
            s["connections"].get<int>(),
            s["snodes"].get<int>(),
            s["pending_connections"].get<int>(),
            s["block_height"].get<int>(),
            s["accounts_monitored"].get<int>(),
            s["subscriptions"]["total"].get<int>(),
            "{}"_format(fmt::join(notifiers, ", ")),
            total_notifies);

    auto sd_format = pre_cmd.empty() ? "STATUS={1}" : "{0}\nSTATUS={1}";
    sd_notify(0, fmt::format(sd_format, pre_cmd, stat_line).c_str());

    if (auto now = std::chrono::steady_clock::now(); now - last_stats_logged >= 4min + 55s) {
        log::info(stats, "Status: {}", stat_line);
        last_stats_logged = now;
    } else {
        log::debug(stats, "Status: {}", stat_line);
    }
}

void HiveMind::on_notifier_validation(
        bool success,
        oxenmq::Message::DeferredSend replier,
        std::string service,
        const SwarmPubkey& pubkey,
        std::shared_ptr<hive::Subscription> sub,
        const std::optional<EncKey>& enc_key,
        std::vector<std::string> data,
        const std::optional<UnsubData>& unsub) {

    // Will have 'error'/'success', 'message', and maybe other things added
    auto response = nlohmann::json::object();
    int code = static_cast<int>(hive::SUBSCRIBE::ERROR);
    std::string message = "Unknown error";

    log::trace(cat, "Received notifier validation ({}/{})", service, success);
    try {
        if (!success) {
            log::critical(
                    cat,
                    "Communication with {} failed: {}",
                    service,
                    "{}"_format(fmt::join(data, " ")));
            if (!data.empty() && data[0] == "TIMEOUT")
                throw hive::subscribe_error{
                        hive::SUBSCRIBE::SERVICE_TIMEOUT,
                        "{} notification service timed out"_format(service)};
            throw hive::subscribe_error{
                    hive::SUBSCRIBE::ERROR,
                    "failed to communicate with {} notification service"_format(service)};
        }

        if (data.size() < 2 || data.size() > 3)
            throw std::invalid_argument{
                    "invalid {}-part response from notification service"_format(data.size())};

        if (int x; parse_int(data[0], x))
            code = x;
        else
            throw std::invalid_argument{"notification service did not give a status code"};

        if (code == static_cast<int>(hive::SUBSCRIBE::OK)) {
            auto service_id = std::move(data[1]);
            if (bool too_short = service_id.size() < SERVICE_ID_MIN_SIZE;
                too_short || service_id.size() > SERVICE_ID_MAX_SIZE)
                throw std::invalid_argument{"service id too {} ({})"_format(
                        too_short ? "short" : "long", service_id.size())};

            code = static_cast<int>(hive::SUBSCRIBE::OK);
            if (!unsub) {  // New/renewed subscription
                assert(sub && enc_key);
                std::optional<std::string> service_data;
                if (data.size() > 2)
                    service_data = std::move(data[2]);
                if (service_data->size() > SERVICE_DATA_MAX_SIZE)
                    throw std::invalid_argument{
                            "service data too long ({})"_format(service_data->size())};
                log::trace(cat, "Adding {} subscription for {}", service, pubkey.id.hex());
                bool newsub = add_subscription(
                        pubkey,
                        std::move(service),
                        std::move(service_id),
                        std::move(service_data),
                        *enc_key,
                        std::move(*sub));
                if (newsub)
                    have_new_subs_ = true;

                response[newsub ? "added" : "updated"] = true;
                message = newsub ? "Subscription successful" : "Resubscription successful";
            } else {  // Unsubscribe
                assert(!sub);
                auto& [sig, subkey_tag, sig_ts] = *unsub;
                bool removed = remove_subscription(
                        pubkey, subkey_tag, std::move(service), std::move(service_id), sig, sig_ts);

                response["removed"] = removed;
                message = removed ? "Device unsubscribed from push notifications"
                                  : "Device was not subscribed to push notifications";
            }
        } else {
            // leave code at whatever the notifier set it to
            message = std::move(data[1]);
        }
    } catch (const hive::subscribe_error& e) {
        code = e.numeric_code();
        message = e.what();
    } catch (const hive::signature_verify_failure& e) {
        code = static_cast<int>(hive::SUBSCRIBE::ERROR);
        message = e.what();
    } catch (const std::exception& e) {
        code = static_cast<int>(hive::SUBSCRIBE::ERROR);
        log::warning(cat, "Exception encountered during sub/unsub handling: {}", e.what());
        message = "An error occured while processing your request";
    }

    if (code == static_cast<int>(hive::SUBSCRIBE::OK))
        response["success"] = true;
    else
        response["error"] = code;
    if (!message.empty())
        response["message"] = std::move(message);

    replier(response.dump());
}

std::tuple<SwarmPubkey, std::optional<SubkeyTag>, int64_t, Signature, std::string, nlohmann::json>
HiveMind::sub_unsub_args(nlohmann::json& args) {

    auto account = from_hex_or_b64<AccountID>(args.at("pubkey").get<std::string_view>());
    std::optional<Ed25519PK> session_ed;
    if (account[0] == static_cast<std::byte>(0x05))
        from_hex_or_b64(session_ed.emplace(), args.at("session_ed25519").get<std::string_view>());
    // SwarmPubkey pubkey{std::move(account), std::move(session_ed)};
    std::optional<SubkeyTag> subkey_tag;
    if (auto it = args.find("subkey_tag"); it != args.end())
        from_hex_or_b64(subkey_tag.emplace(), it->get<std::string_view>());
    auto sig = from_hex_or_b64<Signature>(args.at("signature").get<std::string_view>());

    return {SwarmPubkey{std::move(account), std::move(session_ed)},
            std::move(subkey_tag),
            args.at("sig_ts").get<int64_t>(),
            std::move(sig),
            args.at("service").get<std::string>(),
            args.at("service_info")};
}

oxenmq::ConnectionID HiveMind::sub_unsub_service_conn(const std::string& service) {
    {
        std::lock_guard lock{mutex_};
        if (auto it = services_.find(service); it != services_.end())
            return it->second;
    }
    throw hive::subscribe_error{
            hive::SUBSCRIBE::SERVICE_NOT_AVAILABLE,
            service + " notification service not currently available"};
}

void HiveMind::on_subscribe(oxenmq::Message& m) {
    ready_or_defer();

    // If these are set at the end we send them in reply.
    std::optional<std::pair<hive::SUBSCRIBE, std::string>> error;

    try {
        auto args = nlohmann::json::parse(m.data.at(0));

        auto [pubkey, subkey_tag, sig_ts, sig, service, service_info] = sub_unsub_args(args);

        auto enc_key = from_hex_or_b64<EncKey>(args.at("enc_key").get<std::string_view>());
        auto namespaces = args.at("namespaces").get<std::vector<int16_t>>();

        auto conn = sub_unsub_service_conn(service);

        auto reply_handler = [this,
                              service = service,
                              sub = std::make_shared<hive::Subscription>(  // Throws on bad sig
                                      pubkey,
                                      std::move(subkey_tag),
                                      args.at("namespaces").get<std::vector<int16_t>>(),
                                      args.at("data").get<bool>(),
                                      args.at("sig_ts").get<int64_t>(),
                                      std::move(sig)),
                              pubkey = pubkey,
                              enc_key = std::move(enc_key),
                              replier = m.send_later()](
                                     bool success, std::vector<std::string> data) mutable {
            on_notifier_validation(
                    success,
                    std::move(replier),
                    std::move(service),
                    std::move(pubkey),
                    std::move(sub),
                    std::move(enc_key),
                    std::move(data));
        };

        // We handle everything else (including the response) in `_on_notifier_validation`
        // when/if the notifier service comes back to us with the unique identifier:
        omq_.request(
                conn, "notifier.validate", std::move(reply_handler), service, service_info.dump());

    } catch (const nlohmann::json::exception&) {
        log::debug(cat, "Subscription failed: bad json");
        error = {hive::SUBSCRIBE::BAD_INPUT, "Invalid JSON"};
    } catch (const std::out_of_range& e) {
        log::debug(cat, "Sub failed: missing param {}", e.what());
        error = {hive::SUBSCRIBE::BAD_INPUT, "Missing required parameter"};
    } catch (const hive::subscribe_error& e) {
        error = {e.code, e.what()};
    } catch (const std::exception& e) {
        log::debug(cat, "Exception handling input: {}", e.what());
        error = {hive::SUBSCRIBE::ERROR, e.what()};
    }

    if (error) {
        int code = static_cast<int>(error->first);
        log::debug(cat, "Replying with error code {}: {}", code, error->second);
        m.send_reply(nlohmann::json{{"error", code}, {"message", error->second}}.dump());
    }
    // Otherwise the reply is getting deferred and handled later in on_notifier_validation
}

void HiveMind::on_unsubscribe(oxenmq::Message& m) {
    ready_or_defer();

    // If these are set at the end we send them in reply.
    std::optional<std::pair<hive::SUBSCRIBE, std::string>> error;

    try {
        auto args = nlohmann::json::parse(m.data.at(0));

        auto [pubkey, subkey_tag, sig_ts, sig, service, service_info] = sub_unsub_args(args);

        auto conn = sub_unsub_service_conn(service);

        auto reply_handler = [this,
                              service = service,
                              pubkey = pubkey,
                              unsub = UnsubData{std::move(sig), std::move(subkey_tag), sig_ts},
                              replier = m.send_later()](
                                     bool success, std::vector<std::string> data) mutable {
            on_notifier_validation(
                    success,
                    std::move(replier),
                    std::move(service),
                    std::move(pubkey),
                    nullptr,
                    std::nullopt,
                    std::move(data),
                    std::move(unsub));
        };

        omq_.request(
                conn, "notifier.validate", std::move(reply_handler), service, service_info.dump());

    } catch (const nlohmann::json::exception&) {
        log::debug(cat, "Unsubscription failed: bad json");
        error = {hive::SUBSCRIBE::BAD_INPUT, "Invalid JSON"};
    } catch (const std::out_of_range& e) {
        log::debug(cat, "Unsub failed: missing param {}", e.what());
        error = {hive::SUBSCRIBE::BAD_INPUT, "Missing required parameter"};
    } catch (const hive::subscribe_error& e) {
        error = {e.code, e.what()};
    } catch (const std::exception& e) {
        log::debug(cat, "Exception handling input: {}", e.what());
        error = {hive::SUBSCRIBE::ERROR, e.what()};
    }

    if (error) {
        int code = static_cast<int>(error->first);
        log::debug(cat, "Replying with error code {}: {}", code, error->second);
        m.send_reply(nlohmann::json{{"error", code}, {"message", error->second}}.dump());
    }
    // Otherwise the reply is getting deferred and handled later in on_notifier_validation
}

void HiveMind::db_cleanup() {
    auto conn = pool_.get();
    pqxx::work tx{conn};
    tx.exec_params0(
            "DELETE FROM subscriptions WHERE signature_ts <= $1",
            unix_timestamp(system_clock::now() - SIGNATURE_EXPIRY));
    tx.commit();
}

void HiveMind::refresh_sns() {
    omq_.request(
            oxend_,
            "rpc.get_service_nodes",
            [this](bool success, std::vector<std::string> data) {
                if (success) {
                    on_sns_response(std::move(data));
                } else {
                    log::warning(
                            cat,
                            "get_service_nodes request failed: {}",
                            "{}"_format(fmt::join(data, " ")));
                }
            },
            _get_sns_params);
}

void HiveMind::on_sns_response(std::vector<std::string> data) {
    try {
        if (data.size() != 2) {
            log::warning(
                    cat,
                    "rpc.get_service_nodes returned unexpected {}-length response",
                    data.size());
            return;
        }
        if (data[0] != "200") {
            log::warning(
                    cat,
                    "rpc.get_service_nodes returned unexpected response {}: {}",
                    data[0],
                    data[1]);
            return;
        }

        nlohmann::json res;
        try {
            res = nlohmann::json::parse(data[1]);
        } catch (const nlohmann::json::exception& e) {
            log::warning(cat, "Failed to parse rpc.get_service_nodes response: {}", e.what());
            return;
        }

        auto sn_st = res["service_node_states"];
        if (!sn_st.is_array()) {
            log::warning(
                    cat,
                    "Unexpected rpc.get_service_nodes response: service_node_states looks "
                    "wrong");
            return;
        }

        std::unique_lock lock{mutex_};

        bool swarms_changed = false;
        auto new_hash = res.at("block_hash").get<std::string>();
        auto new_height = res.at("height").get<int64_t>();
        if (new_hash != last_block_.first) {
            log::debug(cat, "new block {} @ {}", new_hash, new_height);

            // The block changed, so we need to check for swarm changes as well
            std::set<uint64_t> new_swarm_ids;
            for (auto& sn : sn_st) {
                auto sw_id = sn.at("swarm_id").get<uint64_t>();
                if (sw_id != INVALID_SWARM_ID)
                    new_swarm_ids.insert(sw_id);
            }
            if (!std::equal(
                        new_swarm_ids.begin(),
                        new_swarm_ids.end(),
                        swarm_ids_.begin(),
                        swarm_ids_.end())) {
                swarms_changed = true;

                swarm_ids_.clear();
                swarm_ids_.insert(swarm_ids_.end(), new_swarm_ids.begin(), new_swarm_ids.end());
            }

            last_block_ = {std::move(new_hash), new_height};
        }

        std::unordered_map<X25519PK, std::tuple<std::string, uint16_t, uint64_t>> sns;
        sns.reserve(sn_st.size());
        for (const auto& s : sn_st) {
            auto pkx = s.at("pubkey_x25519").get<std::string_view>();
            auto ip = s.at("public_ip").get<std::string_view>();
            auto port = s.at("storage_lmq_port").get<uint16_t>();
            auto swarm = s.at("swarm_id").get<uint64_t>();

            if (pkx.size() == 64 && !ip.empty() && ip != "0.0.0.0" && port > 0 &&
                swarm != INVALID_SWARM_ID)
                sns.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(from_hex_or_b64<X25519PK>(pkx)),
                        std::forward_as_tuple(std::move(ip), port, swarm));
        }

        // auto missing_count = sn_st.size() - sns.size();
        log::debug(
                cat, "{} active SNs ({} missing details)", sns.size(), sn_st.size() - sns.size());

        // Anything in self.sns but not in sns is no longer on the network (decommed, dereged,
        // expired), or possibly we lost info for it (from the above).  We're going to
        // disconnect from these (if any are connected).
        int dropped = 0;
        for (auto it = sns_.begin(); it != sns_.end();) {
            const auto& [xpk, snode] = *it;
            if (sns.count(xpk)) {
                ++it;
                continue;
            }

            log::debug(cat, "Disconnecting {}", xpk);
            swarms_[snode->swarm].erase(snode);
            snode->disconnect();
            it = sns_.erase(it);
            dropped++;
        }

        std::unordered_set<std::shared_ptr<hive::SNode>> new_or_changed_sns;

        for (const auto& [xpk, details] : sns) {
            const auto& [ip, port, swarm] = details;
            oxenmq::address addr{"tcp://{}:{}"_format(ip, port), as_sv(xpk.view())};

            if (auto it = sns_.find(xpk); it != sns_.end()) {
                // We already know about this service node from the last update, but it might
                // have changed address or swarm, in which case we want to disconnect and then
                // store it as "new" so that we reconnect to it (if required) later.  (We don't
                // technically have to reconnect if swarm changes, but it simplifies things a
                // bit to do it anyway).
                auto& snode = it->second;
                if (snode->swarm != swarm) {
                    swarms_[snode->swarm].erase(snode);
                    snode->reset_swarm(swarm);
                    swarms_[swarm].insert(snode);
                    new_or_changed_sns.insert(snode);
                }

                // Update the address; this reconnects if the address has changed, does nothing
                // otherwise.
                snode->connect(std::move(addr));
            } else {
                // If we are using separate oxenmq instances for push handling then select the next
                // one, round-robin style:
                if (!omq_push_.empty() && omq_push_next_ == omq_push_.end())
                    omq_push_next_ = omq_push_.begin();

                auto& omq_instance = omq_push_.empty() ? omq_ : *omq_push_next_++;
                // New snode
                auto snode =
                        std::make_shared<hive::SNode>(*this, omq_instance, std::move(addr), swarm);
                sns_.emplace(xpk, snode);
                swarms_[swarm].insert(snode);
                new_or_changed_sns.insert(snode);
            }
        }

        for (auto it = swarms_.begin(); it != swarms_.end();) {
            if (it->second.empty())
                it = swarms_.erase(it);
            else
                ++it;
        }

        log::debug(
                cat, "{} new/updated SNs; dropped {} old SNs", new_or_changed_sns.size(), dropped);

        // If we had a change to the network's swarms then we need to trigger a full recheck of
        // swarm membership, ejecting any pubkeys that moved while adding all pubkeys again to
        // be sure they are in each(possibly new) slot.
        if (swarms_changed) {
            int sw_changes = 0;
            // Recalculate the swarm id of all subscribers:
            for (auto& [pk, v] : subscribers_)
                sw_changes += pk.update_swarm(swarm_ids_);

            log::debug(cat, "{} accounts changed swarms", sw_changes);

            oxenmq::Batch<void> batch;
            batch.reserve(swarms_.size());
            for (auto& [swid, snodes] : swarms_) {
                batch.add_job([this, swid = swid, s = &snodes] {
                    for (auto& sn : *s)
                        sn->remove_stale_swarm_members(swarm_ids_);
                    for (auto& [swarmpk, v] : subscribers_)
                        if (swarmpk.swarm == swid)
                            for (auto& sn : *s)
                                sn->add_account(swarmpk);
                });
            }
            // We release the lock *without* unlocking it below, then deal with finally unlocking
            // it in the completion function when we finish at the end of the batch job.
            batch.completion([this](auto&&) mutable {
                std::unique_lock lock{mutex_, std::adopt_lock};
                check_subs();
            });

            omq_.batch(std::move(batch));

            // Leak the lock:
            lock.release();

        } else if (!new_or_changed_sns.empty()) {
            // Otherwise swarms stayed the same(which means no accounts changed swarms), but
            // snodes might have moved in / out of existing swarms, so re-add any subscribers to
            // swarm changers to ensure they have all the accounts that belong to them.

            std::unordered_map<uint64_t, std::vector<SwarmPubkey>> swarm_subs;
            for (const auto& snode : new_or_changed_sns)
                swarm_subs[snode->swarm];  // default-construction side effect

            for (auto& [swarmpk, v] : subscribers_) {
                if (auto it = swarm_subs.find(swarmpk.swarm); it != swarm_subs.end())
                    it->second.push_back(swarmpk);

                for (const auto& snode : new_or_changed_sns)
                    for (const auto& swarmpk : swarm_subs[snode->swarm])
                        snode->add_account(swarmpk);
            }

            check_subs();
        }
    } catch (const std::exception& e) {
        log::warning(cat, "An exception occured while processing the SN update: {}", e.what());
    }
}

// Re-checks all SN subscriptions; the mutex must be held externally.
void HiveMind::check_subs(bool fast) {
    for (const auto& [xpk, snode] : sns_) {
        try {
            snode->check_subs(subscribers_, false, fast);
        } catch (const std::exception& e) {
            log::warning(cat, "Failed to check subs on {}: {}", xpk, e.what());
        }
    }
}

void HiveMind::check_my_subs(hive::SNode& snode, bool initial) {
    std::lock_guard lock{mutex_};
    snode.check_subs(subscribers_, initial);
}

void HiveMind::subs_slow() {
    // Ignore the confirm response from this; we can't really do anything with it, we just want
    // to make sure we stay subscribed.
    omq_.request(oxend_, "sub.block", nullptr);

    {
        std::lock_guard lock{mutex_};
        check_subs();
    }
}

void HiveMind::subs_fast() {
    if (have_new_subs_.exchange(false)) {
        std::lock_guard lock{mutex_};
        check_subs(true);
    }
}

void HiveMind::finished_connect() {
    bool try_more = pending_connects_ >= config.max_pending_connects;
    log::trace(cat, "finished connection; {}triggering more", try_more ? "" : "not ");
    --pending_connects_;
    if (try_more) {
        std::lock_guard lock{mutex_};
        check_subs();
    }
}

bool HiveMind::allow_connect() {
    int count = ++pending_connects_;
    if (count > config.max_pending_connects) {
        --pending_connects_;
        return false;
    }
    ++connect_count_;
    log::debug(
            cat,
            "establishing connection (currently have {} pending, {} total connects)",
            pending_connects_,
            connect_count_);
    return true;
}

void HiveMind::load_saved_subscriptions() {

    // mutex_ lock not needed: we are only ever called before oxenmq startup in the constructor
    // (i.e.  before there are any other threads to worry about).

    auto started = steady_clock::now();
    auto last_print = started;

    auto conn = pool_.get();
    pqxx::work txn{conn};

    auto [total] = txn.query1<int64_t>("SELECT COUNT(*) FROM subscriptions");
    log::info(cat, "Loading {} stored subscriptions from database", total);

    int64_t count = 0, unique = 0;
    for (auto [acc, ed, tag, sig, sigts, wd, ns_arr] : txn
                                                               .stream<AccountID,
                                                                       std::optional<Ed25519PK>,
                                                                       std::optional<SubkeyTag>,
                                                                       Signature,
                                                                       int64_t,
                                                                       bool,
                                                                       Int16ArrayLoader>(R"(
SELECT account, session_ed25519, subkey_tag, signature, signature_ts, want_data,
    ARRAY(SELECT namespace FROM sub_namespaces WHERE subscription = id ORDER BY namespace)
FROM subscriptions)")) {
        auto [it, ins] = subscribers_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(std::move(acc), std::move(ed), /*_skip_validation=*/true),
                std::forward_as_tuple());

        // Weed out potential duplicates: if two+ devices are subscribed to the same
        // account with all the same relevant subscription settings then we can just
        // keep whichever one is newer.
        bool dupe = false;
        for (auto& existing : it->second) {
            if (existing.is_same(tag, ns_arr.a, wd)) {
                if (sigts > existing.sig_ts) {
                    existing.sig_ts = sigts;
                    existing.sig = std::move(sig);
                }
                dupe = true;
                break;
            }
        }

        if (!dupe) {
            unique++;
            it->second.emplace_back(
                    it->first,
                    std::move(tag),
                    std::move(ns_arr.a),
                    wd,
                    sigts,
                    std::move(sig),
                    /*_skip_validation=*/true);
        }

        if (++count % 100000 == 0) {
            auto now = steady_clock::now();
            auto elapsed = now - last_print;
            if (elapsed >= 1s) {
                log::info(cat, "... processed {}/{} subscriptions", count, total);
                last_print = now;
            }
        }
    }

    log::info(
            cat,
            "Done loading saved subscriptions; {} unique subscriptions to {} accounts",
            unique,
            subscribers_.size());
}

/// Add or updates a subscription for monitoring.  If the given pubkey is already
/// monitored by the same given subkey (if applicable) and same namespace/data
/// values then this replaces the existing subscription, otherwise it adds a new
/// subscription.
///
/// Will throw if the given data or signatures are incorrect.
///
/// Returns true if the subscription was brand new, false if the subscription
/// updated/renewed an existing subscription.
///
/// Parameters:
///
/// - pubkey -- the account to monitor
/// - service -- the subscription service name, e.g. 'apns', 'firebase'.  When
/// messages are
///   received the notification will be forwarded to the given service, if active.
/// - service_id -- an identifier string that identifies the device/application/etc.
/// This must
///   be unique for a given service and pubkey (if all three match, an existing
///   subscription will be replaced).
/// - service_data -- service data; this will be passed as-is to the service handler
///   and contains any extra data (beyond just the service_id) needed for the
///   service handler to send the notification to the device.
/// - enc_key this user's 32-byte encryption key for pushed notifications
/// - sub -- the subscription to add
bool HiveMind::add_subscription(
        SwarmPubkey pubkey,
        std::string service,
        std::string service_id,
        std::optional<std::string> service_data,
        EncKey enc_key,
        hive::Subscription sub) {

    bool new_sub = false, insert_ns = false;

    auto conn = pool_.get();
    pqxx::work tx{conn};

    auto result = tx.query01<int64_t, std::optional<SubkeyTag>, int64_t, Int16ArrayLoader>(
            R"(
SELECT
    id,
    subkey_tag,
    signature_ts,
    ARRAY(SELECT namespace FROM sub_namespaces WHERE subscription = id ORDER BY namespace)
FROM subscriptions
WHERE
    account = {} AND service = {} AND svcid = {})"_format(
                    tx.quote(pubkey.id), tx.quote(service), tx.quote(service_id)));
    int64_t id;
    if (result) {
        auto& [row_id, subkey_tag, sig_ts, ns_arr] = *result;
        id = row_id;

        insert_ns = ns_arr.a != sub.namespaces;
        log::trace(cat, "updating subscription for {}", pubkey.id.hex());
        tx.exec_params0(
                R"(
UPDATE subscriptions
SET session_ed25519 = $2, subkey_tag = $3, signature = $4, signature_ts = $5, want_data = $6, enc_key = $7, svcdata = $8
WHERE id = $1
                    )",
                id,
                pubkey.session_ed ? std::optional{pubkey.ed25519} : std::nullopt,
                sub.subkey_tag,
                sub.sig,
                sub.sig_ts,
                sub.want_data,
                enc_key,
                service_data);
        if (insert_ns)
            tx.exec_params0("DELETE FROM sub_namespaces WHERE subscription = $1", id);
    } else {
        new_sub = true;
        log::trace(cat, "inserting new subscription for {}", pubkey.id.hex());
        auto row = tx.exec_params1(
                R"(
INSERT INTO subscriptions
    (account, session_ed25519, subkey_tag, signature, signature_ts, want_data, enc_key, service, svcid, svcdata)
VALUES ($1,   $2,              $3,         $4,        $5,           $6,        $7,      $8,      $9,    $10)
RETURNING id
                )",
                pubkey.id,
                pubkey.session_ed ? std::optional{pubkey.ed25519} : std::nullopt,
                sub.subkey_tag,
                sub.sig,
                sub.sig_ts,
                sub.want_data,
                enc_key,
                service,
                service_id,
                service_data);

        id = row[0].as<int64_t>();
        insert_ns = true;
    }

    if (insert_ns)
        for (auto n : sub.namespaces)
            tx.exec_params0(
                    R"(INSERT INTO sub_namespaces (subscription, namespace) VALUES ($1, $2))",
                    id,
                    n);

    for (const auto& s : {""s, service})
        increment_stat(tx, s, new_sub ? "subscription" : "sub_renew", 1);

    tx.commit();

    std::lock_guard lock{mutex_};
    pubkey.update_swarm(swarm_ids_);

    auto& subscriptions = subscribers_[pubkey];
    bool found_existing = false;
    for (auto& existing : subscriptions) {
        if (existing.is_same(sub)) {
            if (sub.is_newer(existing)) {
                existing.sig = sub.sig;
                existing.sig_ts = sub.sig_ts;
            }
            found_existing = true;
            break;
        }
    }
    if (!found_existing)
        subscriptions.push_back(std::move(sub));

    // If this is actually adding a new subscription (and not just renewing an
    // existing one) then we need to force subscription (or resubscription) on all
    // of the account's swarm members to get the subscription active ASAP.
    // (Otherwise don't do anything because we already have an equivalent
    // subscription in place).
    if (new_sub)
        for (auto& sn : swarms_[pubkey.swarm])
            sn->add_account(pubkey, /*force_now=*/true);

    return new_sub;
}

/// Removes a subscription for monitoring.  Returns true if the given pubkey was
/// found and removed; false if not found.
///
/// Will throw if the given data or signatures are incorrect.
///
/// Parameters:
///
/// - pubkey -- the account
/// - subkey_tag -- if using subkey authentication then this is the 32-byte subkey
/// tag.
/// - service -- the subscription service name, e.g. 'apns', 'firebase'.
/// - service_id -- an identifier string that identifies the device/application/etc.
/// This is
///   unique for a given service and pubkey and is generated/extracted by the
///   notification service.
/// - sig_ts -- the integer unix timestamp when the signature was generated; must be
/// within ±24h
/// - signature -- the Ed25519 signature of: UNSUBSCRIBE || PUBKEY_HEX || sig_ts
bool HiveMind::remove_subscription(
        const SwarmPubkey& pubkey,
        const std::optional<SubkeyTag>& subkey_tag,
        std::string service,
        std::string service_id,
        const Signature& sig,
        int64_t sig_ts) {

    if (sig_ts < unix_timestamp(system_clock::now() - UNSUBSCRIBE_GRACE) ||
        sig_ts > unix_timestamp(system_clock::now() + UNSUBSCRIBE_GRACE))
        throw std::invalid_argument{"Invalid signature: sig_ts is too far from current time"};

    // "UNSUBSCRIBE" || HEX(ACCOUNT) || SIG_TS
    std::string sig_msg = "UNSUBSCRIBE";
    oxenc::to_hex(pubkey.id.begin(), pubkey.id.end(), std::back_inserter(sig_msg));
    fmt::format_to(std::back_inserter(sig_msg), "{}", sig_ts);

    // Throws on verification failure
    hive::verify_storage_signature(sig_msg, sig, pubkey.ed25519, subkey_tag);

    bool removed = false;

    auto conn = pool_.get();
    pqxx::work tx{conn};

    auto result = tx.exec_params0(
            R"(DELETE FROM subscriptions WHERE account = $1 AND service = $2 AND svcid = $3)",
            pubkey.id,
            service,
            service_id);

    tx.commit();

    // We don't remove the subscription from internal data structures: other devices
    // (with the exact subcription) may be still using it, and so we may still want
    // the notifications; but as long as the row is removed (above) we won't be
    // sending notifications to the device anymore.
    return result.affected_rows() > 0;
}

}  // namespace spns
