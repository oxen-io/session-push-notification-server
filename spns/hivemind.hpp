#pragma once

//
// This contains the oxenmq "Hive Mind" process that establishes connections to all the network's
// service nodes, maintaining subscriptions with them for all the users that have enabled push
// notifications and processing incoming message notifications from those SNs.
//
// The hivemind instance runs in its own process and keeps open bidirection oxenmq connections with
// the notifiers (for proxying notifications) and uwsgi processes (for receiving client subscription
// updates).
//

#include <oxenmq/connections.h>
#include <oxenmq/message.h>
#include <oxenmq/oxenmq.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <nlohmann/json_fwd.hpp>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bytes.hpp"
#include "config.hpp"
#include "hive/snode.hpp"
#include "hive/subscription.hpp"
#include "pg.hpp"
#include "swarmpubkey.hpp"
#include "utils.hpp"

namespace spns {

// How long until we expire subscriptions (relative to the signature timestamp).  This can be no
// more than 14 days (because that's the subscription cutoff for storage server), but can also be
// less.
inline constexpr auto SIGNATURE_EXPIRY = 14 * 24h;

// How much we allow an unsubscribe signature timestamp to be off before we reject it
inline constexpr auto UNSUBSCRIBE_GRACE = 24h;

inline constexpr size_t MSG_HASH_MIN_SIZE = 32;
inline constexpr size_t MSG_HASH_MAX_SIZE = 99;
inline constexpr size_t SERVICE_NAME_MAX_SIZE = 32;
inline constexpr size_t SERVICE_ID_MIN_SIZE = 32;
inline constexpr size_t SERVICE_ID_MAX_SIZE = 999;
inline constexpr size_t SERVICE_DATA_MAX_SIZE = 99'999;
inline constexpr size_t MSG_DATA_MAX_SIZE = 76'800;  // Storage server limit

// Parameters for our request to get service node data:
inline constexpr auto _get_sns_params = R"({
  "active_only": true,
  "fields": {
    "pubkey_x25519": true,
    "public_ip": true,
    "storage_lmq_port": true,
    "swarm_id": true,
    "block_hash": true,
    "height": true
  }
})";

// Thrown when startup is not ready; we catch it in ExcWrapper, below, and defer it to be called as
// soon as startup completes.
struct startup_request_defer {};

/// Callable object that wraps a HiveMind on_whatever method, invoking it inside an exception
/// handler that, upon uncaught exception, produces a log error and (if a json request endpoint)
/// replies with a generic error code.
class ExcWrapper {
  private:
    HiveMind& hivemind;
    void (HiveMind::*const meth)(oxenmq::Message&);
    const std::string meth_name;
    bool is_json_request;

  public:
    ExcWrapper(
            HiveMind& hivemind,
            void (HiveMind::*meth)(oxenmq::Message&),
            std::string meth_name,
            bool is_json_request = false) :
            hivemind{hivemind},
            meth{meth},
            meth_name{std::move(meth_name)},
            is_json_request{is_json_request} {}

    void operator()(oxenmq::Message& m);
};

// If requests arrive during startup we copy the request here to defer calling until after
// startup completes.
struct DeferredRequest {
    oxenmq::Message message;
    std::vector<std::string> data;
    ExcWrapper& callback;

    DeferredRequest(oxenmq::Message&& m, ExcWrapper& callback);

    void operator()() && { callback(message); }
};

class HiveMind {

  public:
    const Config config;

  private:
    std::mutex mutex_;
    oxenmq::OxenMQ omq_;
    PGConnPool pool_;

    // xpk -> SNode
    std::unordered_map<X25519PK, std::shared_ptr<hive::SNode>> sns_;
    // swarmid -> {SNode...}
    std::unordered_map<uint64_t, std::unordered_set<std::shared_ptr<hive::SNode>>> swarms_;

    // Sorted list of all swarm ids
    std::vector<uint64_t> swarm_ids_;

    // {swarmpubkey: [Subscription...]} -- all subscriptions, per account (less dupes)
    std::unordered_map<SwarmPubkey, std::vector<hive::Subscription>> subscribers_;

    // last block hash & height
    std::pair<std::string, int64_t> last_block_{"", -1};

    std::atomic<int> pending_connects_ = 0;
    std::atomic<int> connect_count_ = 0;

    const std::chrono::system_clock::time_point startup_time = std::chrono::system_clock::now();

    // contains Blake2B(service || svcid || msghash) for sent notification de-duping.  Every 10
    // minutes, we replace filter_rotate_ with filter_, and check both filters for de-duping (so
    // that hashes expire after 10-20 minutes).  (The 10 mins value is configurable).
    std::unordered_set<Blake2B_32> filter_, filter_rotate_;
    steady_time filter_rotate_time_ = steady_clock::now() + config.filter_lifetime;

    // Registered push services: servicename => omq Connectionid to talk to the service
    std::unordered_map<std::string, oxenmq::ConnectionID> services_;

    // Our connection to a local oxend for block and SN info
    oxenmq::ConnectionID oxend_;

    // Will be set to true once we are ready to start taking requests
    std::atomic<bool> ready{false};

    // Set to true if we have new subs we need to deal with ASAP
    std::atomic<bool> have_new_subs_{false};

    void ready_or_defer() {
        if (!ready)
            throw startup_request_defer{};
    }
    void set_ready();

    bool notifier_startup_done(const steady_time& wait_until);

    friend class ExcWrapper;
    std::mutex deferred_mutex_;
    std::list<DeferredRequest> deferred_;
    void defer_request(oxenmq::Message&& m, ExcWrapper& callback);

  public:
    HiveMind(Config conf_);

  private:
    void on_reg_service(oxenmq::Message& m);

    void on_message_notification(oxenmq::Message& m);

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
    void on_service_stats(oxenmq::Message& m);

    void on_get_stats(oxenmq::Message& m);

    using UnsubData = std::tuple<Signature, std::optional<SubkeyTag>, int64_t>;
    void on_notifier_validation(
            bool success,
            oxenmq::Message::DeferredSend replier,
            std::string service,
            const SwarmPubkey& pubkey,
            std::shared_ptr<hive::Subscription> sub,
            const std::optional<EncKey>& enc_key,
            std::vector<std::string> data,
            const std::optional<UnsubData>& unsub = std::nullopt);

    std::tuple<
            SwarmPubkey,
            std::optional<SubkeyTag>,
            int64_t,
            Signature,
            std::string,
            nlohmann::json>
    sub_unsub_args(nlohmann::json& args);

    oxenmq::ConnectionID sub_unsub_service_conn(const std::string& service);

    void on_subscribe(oxenmq::Message& m);

    void on_unsubscribe(oxenmq::Message& m);

    void db_cleanup();

    void on_new_block(oxenmq::Message&) { refresh_sns(); }
    void refresh_sns();

    void on_sns_response(std::vector<std::string> data);

    // Re-checks all SN subscriptions; the mutex must be held externally.  `fast` is whether this is
    // a quick, only-new-subs check or a regular check.
    void check_subs(bool fast = false);

    void subs_slow();
    void subs_fast();

  public:
    /// Called when initiating a connection: if this returns a evaluates-as-true object then the
    /// connection can proceed; if it returns false then the connection should not.
    ///
    /// If this returns true then the caller must call `finished_connect` when done connecting
    /// (whether successful or not).
    bool allow_connect();
    void finished_connect();

    // Called *without* the mutex to check the subs of a single snode; this is typically called from
    // within hive::SNode after first connecting.
    void check_my_subs(hive::SNode& snode, bool initial);

    void load_saved_subscriptions();

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
    bool add_subscription(
            SwarmPubkey pubkey,
            std::string service,
            std::string service_id,
            std::optional<std::string> service_data,
            EncKey enc_key,
            hive::Subscription sub);

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
    bool remove_subscription(
            const SwarmPubkey& pubkey,
            const std::optional<SubkeyTag>& subkey_tag,
            std::string service,
            std::string service_id,
            const Signature& sig,
            int64_t sig_ts);
};

}  // namespace spns
