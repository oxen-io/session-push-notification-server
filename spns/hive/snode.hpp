#pragma once

#include <oxenmq/oxenmq.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "subscription.hpp"

namespace spns {
class HiveMind;
}

namespace spns::hive {

using namespace std::literals;

// Maximum size of simultaneous subscriptions in a single subscription request; if we overflow then
// any stragglers wait until the next request, delaying them by a few seconds.  (This is not a rock
// hard limit: we estimate slightly and stop as soon as we exceed it, which means we can go over it
// a bit after appending the last record).
inline constexpr size_t SUBS_REQUEST_LIMIT = 5'000'000;

// How long (in seconds) after a successful subscription before we re-subscribe; each subscription
// gets a uniform random value between these two values (to spread out the renewal requests a bit).
inline constexpr std::chrono::seconds RESUBSCRIBE_MIN = 45min;
inline constexpr std::chrono::seconds RESUBSCRIBE_MAX = 55min;

// How long we wait (in seconds) after a connection failure to a snode storage server before
// re-trying the connection; we use the first value after the first failure, the second one after
// the second failure, and so on (if we run off the end we use the last value).
inline constexpr std::array CONNECT_COOLDOWN = {10s, 30s, 60s, 120s};

template <typename T, typename = std::enable_if_t<is_bytes<T>>>
inline std::string_view as_sv(const T& data) {
    return {reinterpret_cast<const char*>(data.data()), T::SIZE};
}

class SNode {
    // Class managing a connection to a single service node

    HiveMind& hivemind_;
    oxenmq::OxenMQ& omq_;
    oxenmq::ConnectionID conn_;
    oxenmq::address addr_;
    std::atomic<bool> connected_ = false;
    std::unordered_set<SwarmPubkey> subs_;
    uint64_t swarm_;

    std::mutex mutex_;  // Mutex for our local stuff; we must *never* do something with hivemind
                        // that requires a lock while we hold this.

    using system_clock = std::chrono::system_clock;
    using steady_clock = std::chrono::steady_clock;
    using system_time = system_clock::time_point;
    using steady_time = steady_clock::time_point;
    inline static constexpr system_time system_epoch{};

    // Sorted by next re-subscription time.  We reset the pubkey as a means of lazy deferred queue
    // entry deletion (when processing the queue, we just skip such entries).
    std::deque<std::pair<std::optional<SwarmPubkey>, system_time>> next_;

    std::optional<steady_time> cooldown_until_;
    int cooldown_fails_ = 0;

  public:
    const uint64_t& swarm{swarm_};

    SNode(HiveMind& hivemind, oxenmq::OxenMQ& omq, oxenmq::address addr, uint64_t swarm);

    ~SNode() { disconnect(); }

    /// Checks the given address against the current one: if different, it gets replaced, the
    /// current connection (if any) is disconnected, and then we initiate reconnection to the new
    /// address.
    ///
    /// Does nothing if already connected to the given address.
    void connect(oxenmq::address addr);

    /// Initiates a connection, if not already connected, to the current address.
    void connect();

    bool connected() { return connected_; }

    void disconnect();

    void on_connected(oxenmq::ConnectionID c);

    void on_connect_fail(oxenmq::ConnectionID c, std::string_view reason);

    /// Adds a new account to be signed up for subscriptions, if it is not already subscribed.
    /// The new account's subscription will be submitted to the SS the next time check_subs() is
    /// called (either automatically or manually).
    ///
    /// If `force_now` is True then the account is scheduled for subscription at the next update
    /// even if already exists.
    void add_account(const SwarmPubkey& account, bool force_now = false);

    /// Called when this snode's swarm changes; all current subscriptions are dropped.
    void reset_swarm(uint64_t new_swarm);

    /// Called when the network swarm list has changed to eject any swarm subscriptions that don't
    /// belong here anymore.  Any existing subscribers that are no longer in this swarm will be
    /// removed.  (Even without a swarm change of this node, this can happen if another new swarm is
    /// created next to us).
    ///
    /// This isn't responsible for adding *new* swarm members: this is just called as a first step
    /// for removing any that shouldn't be here anymore.
    void remove_stale_swarm_members(const std::vector<uint64_t>& swarm_ids);

    /// Check our subscriptions to resubscribe to any that need it.  Takes a reference to hivemind's
    /// master list of all subscriptions (to be able to pull subscription details from).
    ///
    /// If initial_subs is true then this is the initial request and we fire off a batch of
    /// subscriptions and then another batch upon reply, etc. until there are no more subs to send;
    /// otherwise we fire off just up to SUBS_LIMIT re-subscriptions.
    ///
    /// If `fast` is true then we only look for and process unix-epoch leading elements, which are
    /// the ones we put on we a brand new subscription comes in.
    ///
    /// This method is *only* called from HiveMind.
    void check_subs(
            const std::unordered_map<SwarmPubkey, std::vector<hive::Subscription>>& subs,
            bool initial_subs = false,
            bool fast = false);
};

}  // namespace spns::hive
