#include "subscription.hpp"

#include <oxenc/endian.h>
#include <oxenc/hex.h>
#include <sodium/crypto_core_ed25519.h>
#include <sodium/crypto_generichash_blake2b.h>
#include <sodium/crypto_scalarmult_ed25519.h>
#include <sodium/crypto_sign_ed25519.h>

#include <array>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace spns::hive {

template <typename Int>
static void append_int(std::string& s, Int val) {
    char sig_ts_buf[20];
    auto [end, ec] = std::to_chars(std::begin(sig_ts_buf), std::end(sig_ts_buf), val);
    s.append(sig_ts_buf, end - sig_ts_buf);
}

Subscription::Subscription(
        const SwarmPubkey& pubkey,
        std::optional<SubkeyTag> subkey_tag_,
        std::vector<int16_t> namespaces_,
        bool want_data_,
        int64_t sig_ts_,
        Signature sig_,
        bool _skip_validation) :

        subkey_tag{std::move(subkey_tag_)},
        namespaces{std::move(namespaces_)},
        want_data{want_data_},
        sig_ts{sig_ts_},
        sig{std::move(sig_)} {

    if (namespaces.empty())
        throw std::invalid_argument{"Subscription: namespaces missing or empty"};

    for (size_t i = 0; i < namespaces.size() - 1; i++) {
        if (namespaces[i] > namespaces[i + 1])
            throw std::invalid_argument{"Subscription: namespaces are not sorted numerically"};
        if (namespaces[i] == namespaces[i + 1])
            throw std::invalid_argument{"Subscription: namespaces contains duplicates"};
    }

    if (!sig_ts)
        throw std::invalid_argument{"Subscription: signature timestamp is missing"};
    auto now = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
    if (sig_ts <= now - 14 * 24 * 60 * 60)
        throw std::invalid_argument{"Subscription: sig_ts timestamp is too old"};
    if (sig_ts >= now + 24 * 60 * 60)
        throw std::invalid_argument{"Subscription: sig_ts timestamp is too far in the future"};

    if (!_skip_validation) {
        std::string sig_msg;
        sig_msg.reserve(7 + 66 + 10 + 1 + 7 * namespaces.size() - 1);
        sig_msg += "MONITOR";
        oxenc::to_hex(pubkey.id.begin(), pubkey.id.end(), std::back_inserter(sig_msg));
        append_int(sig_msg, sig_ts);
        sig_msg += want_data ? '1' : '0';
        for (size_t i = 0; i < namespaces.size(); i++) {
            if (i > 0)
                sig_msg += ',';
            append_int(sig_msg, namespaces[i]);
        }

        std::optional<Ed25519PK> verify_pubkey;
        if (subkey_tag) {
            auto& vpk = verify_pubkey.emplace();
            // H("OxenSSSubkey" || c || A):
            crypto_generichash_blake2b_state state;
            crypto_generichash_blake2b_init(
                    &state, reinterpret_cast<const unsigned char*>("OxenSSSubkey"), 12, 32);
            crypto_generichash_blake2b_update(&state, *subkey_tag, 32);
            crypto_generichash_blake2b_update(&state, pubkey.ed25519, 32);
            crypto_generichash_blake2b_final(&state, vpk, 32);

            // c + H(...):
            crypto_core_ed25519_scalar_add(vpk, vpk, pubkey.ed25519);

            // (c + H(...)) A:
            if (0 != crypto_scalarmult_ed25519_noclamp(vpk, vpk, pubkey.ed25519))
                throw std::invalid_argument{"Invalid pubkey/subkey combination"};
        }

        if (0 != crypto_sign_ed25519_verify_detached(
                         sig,
                         reinterpret_cast<const unsigned char*>(sig_msg.data()),
                         sig_msg.size(),
                         verify_pubkey ? *verify_pubkey : pubkey.ed25519))
            throw std::invalid_argument{"Subscription: signature validation failed"};
    }
}

bool Subscription::covers(const Subscription& other) const {
    if (subkey_tag != other.subkey_tag)
        return false;
    if (other.want_data && !want_data)
        return false;

    // Namespaces are sorted, so we can walk through sequentially, comparing heads, and
    // skipping any extras we have have in self.  We fail by either running out of self
    // namespaces before consuming all the other namespaces (which means other has some
    // greater than self's maximum), or when the head of self is greater than the head of
    // other (which means self is missing some at the beginning or in the middle).
    for (size_t i = 0, j = 0; j < other.namespaces.size(); i++) {
        if (i >= namespaces.size())
            // Ran out of self namespaces before we consumed all the other namespaces
            return false;
        if (namespaces[i] > other.namespaces[j])
            // Head of the self is greater, so we are missing (at least) one of other's
            return false;
        if (namespaces[i] == other.namespaces[j])
            // Equal, so we have it: advance j (as well as i) so that both heads advance
            j++;
        // Otherwise [i] < [j], so just skip `i` but leave `j` alone
    }

    return true;
}

}  // namespace spns::hive
