#include "snode.hpp"

#include <oxenc/bt_producer.h>

#include <algorithm>
#include <cassert>
#include <iterator>
#include <memory>
#include <mutex>
#include <oxen/log.hpp>
#include <random>
#include <string>

#include "../bytes.hpp"
#include "../hivemind.hpp"

namespace spns::hive {

namespace log = oxen::log;

static auto cat = log::Cat("snode");

using namespace std::literals;

thread_local std::mt19937_64 rng{std::random_device{}()};

SNode::SNode(HiveMind& hivemind, oxenmq::OxenMQ& omq, oxenmq::address addr, uint64_t swarm) :
        hivemind_{hivemind},
        omq_{omq},
        addr_{std::move(addr)},
        swarm_{swarm}

{
    connect();
}

void SNode::connect() {
    std::lock_guard lock{mutex_};

    if (!conn_) {
        if (hivemind_.allow_connect()) {
            conn_ = omq_.connect_remote(
                    addr_,
                    [this](oxenmq::ConnectionID c) { on_connected(c); },
                    [this](oxenmq::ConnectionID c, std::string_view err) {
                        on_connect_fail(c, err);
                    },
                    oxenmq::AuthLevel::basic);
            log::debug(cat, "Establishing connection to {}", addr_.full_address());
        }
    }
}

void SNode::connect(oxenmq::address addr) {
    bool reconnect;
    {
        std::lock_guard lock{mutex_};
        reconnect = addr != addr_;
    }

    if (reconnect) {
        log::debug(
                cat,
                "disconnecting; addr changing from {} to {}",
                addr_.full_address(),
                addr.full_address());
        disconnect();
        {
            std::lock_guard lock{mutex_};
            addr_ = std::move(addr);
        }
    }

    connect();
}

void SNode::disconnect() {
    std::lock_guard lock{mutex_};

    log::debug(cat, "disconnecting from {}", addr_.full_address());
    connected_ = false;
    if (conn_) {
        omq_.disconnect(conn_);
        conn_ = {};
    }
}

void SNode::on_connected(oxenmq::ConnectionID c) {
    bool no_conn = false;
    {
        std::lock_guard lock{mutex_};

        log::debug(cat, "Connection established to {}", addr_.full_address());
        cooldown_fails_ = 0;
        cooldown_until_.reset();

        if (!conn_) {
            // Our conn got replaced from under us, which probably means we are disconnecting, so do
            // nothing.
            no_conn = true;
        } else {
            // We either just connected or reconnected, so reset any re-subscription times (so that
            // after a reconnection we force a re-subscription for everyone):
            auto now = system_clock::now();
            for (auto& [id, next] : next_)
                next = system_epoch;

            connected_ = true;
        }
    }

    hivemind_.finished_connect();

    if (!no_conn)
        hivemind_.check_my_subs(*this, true);
}

void SNode::on_connect_fail(oxenmq::ConnectionID c, std::string_view reason) {
    {
        std::lock_guard lock{mutex_};

        auto cooldown = cooldown_fails_ >= CONNECT_COOLDOWN.size()
                              ? CONNECT_COOLDOWN.back()
                              : CONNECT_COOLDOWN[cooldown_fails_];
        cooldown_until_ = steady_clock::now() + cooldown;
        cooldown_fails_++;

        log::warning(
                cat,
                "Connection to {} failed: {} ({} consecutive failure(s); retrying in {}s)",
                addr_.full_address(),
                reason,
                cooldown_fails_,
                cooldown.count());

        connected_ = false;
        conn_ = {};
    }

    hivemind_.finished_connect();
}

/// Adds a new account to be signed up for subscriptions, if it is not already subscribed.
/// The new account's subscription will be submitted to the SS the next time check_subs() is
/// called (either automatically or manually).
///
/// If `force_now` is True then the account is scheduled for subscription at the next update
/// even if already exists.
void SNode::add_account(const SwarmPubkey& account, bool force_now) {
    std::lock_guard lock{mutex_};

    auto [it, inserted] = subs_.insert(account);
    if (inserted)
        next_.emplace_front(*it, system_epoch);
    else if (force_now) {
        // We're asked to treat it as "now", so go look for it in the queue and clear it first,
        // then re-insert at the beginning of the queue.
        for (auto& [acc, next] : next_) {
            if (acc && *acc == account) {
                acc.reset();  // lazy deletion; we'll skip this when draining the queue
                break;
            }
        }
        next_.emplace_front(account, system_epoch);
    }
}

void SNode::reset_swarm(uint64_t new_swarm) {
    std::lock_guard lock{mutex_};

    next_.clear();
    subs_.clear();
    swarm_ = new_swarm;
}

void SNode::remove_stale_swarm_members(const std::vector<uint64_t>& swarm_ids) {
    std::lock_guard lock{mutex_};

    for (auto& s : subs_)
        s.update_swarm(swarm_ids);
    for (auto& [acc, next] : next_) {
        if (acc && acc->swarm != swarm_) {
            subs_.erase(*acc);
            acc.reset();
        }
    }
}

void SNode::check_subs(
        const std::unordered_map<SwarmPubkey, std::vector<hive::Subscription>>& all_subs,
        bool initial_subs,
        bool fast) {
    if (!connected_) {
        {
            std::lock_guard lock{mutex_};

            if (conn_)
                return;  // We're already trying to connect

            // If we failed recently we'll be in cooldown mode for a while, so might not connect
            // right away yet.
            if (cooldown_until_) {
                if (*cooldown_until_ > steady_clock::now())
                    return;
                cooldown_until_.reset();
            }
        }

        // We'll get called automatically as soon as the connection gets established, so just
        // make sure we are already connecting and don't do anything else for now.
        return connect();  // NB: must not hold lock when calling this
    }

    std::string req_body = "l";  // We'll add the "e" later
    auto now = system_clock::now();

    size_t next_added = 0, req_count = 0;

    std::lock_guard lock{mutex_};
    while (req_body.size() < SUBS_REQUEST_LIMIT && !next_.empty()) {
        const auto& [maybe_acct, next] = next_.front();
        if (next > now)
            break;
        if (fast && next > system_epoch)
            break;

        if (!maybe_acct) {
            next_.pop_front();
            continue;  // lazy deletion; ignore this entry
        }

        const auto& acct = *maybe_acct;

        auto subs = all_subs.find(acct);
        if (subs == all_subs.end()) {
            next_.pop_front();
            continue;
        }

        std::vector<char> buf;
        for (const auto& sub : subs->second) {

            // Size estimate; this can be over, but mustn't be under the actual size we'll need.
            constexpr size_t base_size = 0 + 3 +
                                         12      // 1:t and i...e where ... is a 10-digit timestamp
                                       + 3 + 67  // 1:s and 64:...
                                       + 3 + 36  // 1:p and 33:... (also covers 1:P and 32:...)
                                       + 3 + 2   // 1:n and the le of the l...e list
                                       + 3 + 3   // 1:d and i1e (only if want_data)
                                       + 3 + 67 + 3 + 39 // 1:S, 64:..., 1:T, 36:... (for subaccount auth)
                    ;

            // The biggest int expression we have is i-32768e; this is almost certainly overkill
            // most of the time though, but no matter.
            auto size = base_size + sub.namespaces.size() * 8;

            auto old_size = req_body.size();
            req_body.resize(old_size + size);

            char* start = req_body.data() + old_size;

            oxenc::bt_dict_producer dict{start, size};

            // keys in ascii-sorted order!
            if (acct.session_ed)
                dict.append("P", acct.ed25519.sv());
            if (sub.subaccount) {
                dict.append("S", sub.subaccount->sig.sv());
                dict.append("T", sub.subaccount->tag.sv());
            }
            if (sub.want_data)
                dict.append("d", 1);
            dict.append_list("n").append(sub.namespaces.begin(), sub.namespaces.end());
            if (!acct.session_ed)
                dict.append("p", acct.id.sv());
            dict.append("s", sub.sig.sv());
            dict.append("t", sub.sig_ts);

            // Resize away any extra buffer space we didn't fill
            req_body.resize(dict.end() - req_body.data());

            req_count++;
        }

        auto delay = 1s * std::uniform_int_distribution<int>{
                                  RESUBSCRIBE_MIN.count(), RESUBSCRIBE_MAX.count()}(rng);

        next_.emplace_back(acct, now + delay);
        next_added++;
        next_.pop_front();
    }

    if (req_body.size() == 1)  // just the initial "l"
        return;

    req_body += 'e';

    // The randomness of our delay will mean the tail of the list isn't sorted, so re-sort from
    // the lowest possible value we could have inserted (now + RESUBSCRIBE_MIN) to the end.

    // Everything we didn't touch should already be sorted:
    assert(std::is_sorted(
            next_.begin(), std::prev(next_.end(), next_added), [](const auto& a, const auto& b) {
                return a.second < b.second;
            }));

    auto it = std::partition_point(
            next_.begin(),
            std::prev(next_.end(), next_added),
            [&now](const decltype(next_)::value_type& n) {
                return n.second < now + RESUBSCRIBE_MIN;
            });

    std::sort(it, next_.end(), [](const auto& a, const auto& b) { return a.second < b.second; });

    // Now everything should be sorted:
    assert(std::is_sorted(next_.begin(), next_.end(), [](const auto& a, const auto& b) {
        return a.second < b.second;
    }));

    auto on_reply = [this, right_away = initial_subs && req_body.size() >= SUBS_REQUEST_LIMIT](
                            bool success, std::vector<std::string> data) {
        if (!success) {
            // TODO: log something about failed request, but otherwise ignore it.  We don't
            // worry about the subscriptions that might lapse because we have full swarm
            // redundancy so it really doesn't matter if a subscription with one or two of
            // the swarm members times out.
        }
        if (right_away) {
            // We're doing the initial subscriptions, and sent a size-limited request so we
            // likely have more that we want to subscribe to ASAP: so we continue as soon as
            // we get the reply back so that we're subscribing as quickly as possible
            // without having more than one (large) subscription request in flight at a
            // time.
            hivemind_.check_my_subs(*this, true);
        }
    };

    omq_.request(conn_, "monitor.messages", std::move(on_reply), std::move(req_body));
    log::debug(cat, "(Re-)subscribing to {} accounts from {}", req_count, addr_.full_address());
}

}  // namespace spns::hive
