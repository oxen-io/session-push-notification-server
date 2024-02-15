#pragma once

#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "../bytes.hpp"
#include "../swarmpubkey.hpp"

namespace spns::hive {

using namespace std::literals;

enum class SUBSCRIBE : int {
    OK = 0,         // Great Success!
    BAD_INPUT = 1,  // Unparseable, invalid values, missing required arguments, etc. (details in the
                    // string)
    SERVICE_NOT_AVAILABLE = 2,  // The requested service name isn't currently available
    SERVICE_TIMEOUT = 3,        // The backend service did not response
    ERROR = 4,  // There was some other error processing the subscription (details in the string)
    INTERNAL_ERROR = 5,  // An internal program error occured processing the request

    _END,  // Not a proper value; allows easier compile-time checks against new values
};

class subscribe_error : public std::runtime_error {
  public:
    SUBSCRIBE code;
    subscribe_error(SUBSCRIBE code, std::string message) :
            std::runtime_error{message}, code{code} {}

    int numeric_code() const { return static_cast<int>(code); }
};

struct Subscription {
    static constexpr std::chrono::seconds SIGNATURE_EXPIRY{14 * 24h};

    std::optional<Subaccount> subaccount;
    std::vector<int16_t> namespaces;
    bool want_data;
    int64_t sig_ts;
    Signature sig;

    Subscription(
            const SwarmPubkey& pubkey_,
            std::optional<Subaccount> subaccout_,
            std::vector<int16_t> namespaces_,
            bool want_data_,
            int64_t sig_ts_,
            Signature sig_,
            bool _skip_validation = false);

    // Returns true if `this` and `other` represent the same subscription as far as upstream swarm
    // subscription is concerned.  That is: same subaccount tag, same namespaces, and same want_data
    // value.  The caller is responsible for also ensuring that the subscription applies to the same
    // account (i.e. has the same SwarmPubkey).
    bool is_same(const Subscription& other) const {
        return is_same(other.subaccount, other.namespaces, other.want_data);
    }
    // Same as above, but takes the constituent parts.
    bool is_same(
            const std::optional<Subaccount>& o_subaccount,
            const std::vector<int16_t>& o_namespaces,
            bool o_want_data) const {
        return Subaccount::is_same(subaccount, o_subaccount) && namespaces == o_namespaces &&
               want_data == o_want_data;
    }

    // Returns true if `this` subscribes to at least everything needed for `other`; `this` can
    // return extra things (e.g. extra namespaces), but cannot omit anything that `other` needs to
    // send notifications, nor can the two subscriptions use different subaccount tags.  This is
    // *only* valid for two Subscriptions referring to the same account!
    bool covers(const Subscription& other) const;

    bool is_expired(int64_t now) const { return sig_ts < now - SIGNATURE_EXPIRY.count(); }

    bool is_newer(const Subscription& other) const { return sig_ts > other.sig_ts; }
};

}  // namespace spns::hive
