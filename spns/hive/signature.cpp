#include "signature.hpp"

#include <sodium/crypto_core_ed25519.h>
#include <sodium/crypto_generichash_blake2b.h>
#include <sodium/crypto_scalarmult_ed25519.h>
#include <sodium/crypto_sign.h>

#include "../blake2b.hpp"

namespace spns::hive {

void verify_signature(std::string_view sig_msg, const Signature& sig, const Ed25519PK& pubkey) {
    if (0 != crypto_sign_verify_detached(sig, as_usv(sig_msg).data(), sig_msg.size(), pubkey))
        throw signature_verify_failure{"Signature verification failed"};
}

/// Throws signature_verify_failure on signature failure.
void verify_storage_signature(
        std::string_view sig_msg,
        const Signature& sig,
        const Ed25519PK& pubkey,
        const std::optional<SubkeyTag>& subkey_tag) {

    if (subkey_tag) {
        // H(c || A, key="OxenSSSubkey")
        auto verify_pubkey = blake2b_keyed<Ed25519PK>(subkey_tag_hash_key, *subkey_tag, pubkey);

        // c + H(...)
        crypto_core_ed25519_scalar_add(verify_pubkey, *subkey_tag, verify_pubkey);

        // (c + H(...)) A
        if (0 != crypto_scalarmult_ed25519_noclamp(verify_pubkey, verify_pubkey, pubkey))
            throw signature_verify_failure{"Failed to compute subkey: scalarmult failed"};

        verify_signature(sig_msg, sig, verify_pubkey);

    } else {
        verify_signature(sig_msg, sig, pubkey);
    }
}

}  // namespace spns::hive
