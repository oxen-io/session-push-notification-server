#include "signature.hpp"

#include <sodium/crypto_core_ed25519.h>
#include <sodium/crypto_generichash_blake2b.h>
#include <sodium/crypto_scalarmult_ed25519.h>
#include <sodium/crypto_sign.h>

#include "../blake2b.hpp"

namespace spns::hive {

void verify_signature(std::string_view sig_msg, const Signature& sig, const Ed25519PK& pubkey, std::string_view descr) {
    if (0 != crypto_sign_verify_detached(sig, as_usv(sig_msg).data(), sig_msg.size(), pubkey))
        throw signature_verify_failure{std::string{descr} + " verification failed"};
}

namespace {
    constexpr std::byte SUBACC_FLAG_READ{0b0001};
    constexpr std::byte SUBACC_FLAG_ANY_PREFIX{0b1000};
}

/// Throws signature_verify_failure on signature failure.
void verify_storage_signature(
        std::string_view sig_msg,
        const Signature& sig,
        const SwarmPubkey& pubkey,
        const std::optional<Subaccount>& subaccount) {

    if (subaccount) {
        // Parse the subaccount tag:
        // prefix aka netid (05 for session ids, 03 for groups):
        auto prefix = subaccount->tag[0];
        // read/write/etc. flags:
        auto flags = subaccount->tag[1];

        // If you don't have the read bit we can't help you:
        if ((flags & SUBACC_FLAG_READ) == std::byte{0})
            throw signature_verify_failure{"Invalid subaccount: this subaccount does not have read permission"};

        // Unless the subaccount has the "any prefix" flag, check that the prefix matches the
        // account prefix:
        if ((flags & SUBACC_FLAG_ANY_PREFIX) == std::byte{0} &&
                prefix != pubkey.id[0])
            throw signature_verify_failure{"Invalid subaccount: subaccount and main account have mismatched network prefix"};

        // Verify that the main account has signed the subaccount tag:
        verify_signature(subaccount->tag.sv(), subaccount->sig, pubkey.ed25519, "Subaccount auth signature");

        // the subaccount pubkey (starts at [4]; [2] and [3] are future use/null padding):
        Ed25519PK sub_pk;
        std::memcpy(sub_pk.data(), &subaccount->tag[4], 32);

        // Verify that the subaccount pubkey signed this message (and thus is allowed, transitively,
        // since the main account signed the subaccount):
        verify_signature(sig_msg, sig, sub_pk, "Subaccount main signature");

    } else {
        verify_signature(sig_msg, sig, pubkey.ed25519);
    }
}

}  // namespace spns::hive
