#pragma once

#include <stdexcept>

#include "../bytes.hpp"
#include "../utils.hpp"

namespace spns::hive {

using namespace std::literals;

class signature_verify_failure : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// Verifies that the given signature is a valid signature for `sig_msg`.  Supports regular
/// ed25519_pubkey signatures as well as oxen-storage-server derived subkey signatures (if
/// `subkey_tag` is given).

inline constexpr auto subkey_tag_hash_key = "OxenSSSubkey"sv;

// Plain jane Ed25519 signature verification.  Throws a `signature_verify_failure` on verification
// failure.
void verify_signature(std::string_view sig_msg, const Signature& sig, const Ed25519PK& pubkey);

/// Throws signature_verify_failure on signature failure.
void verify_storage_signature(
        std::string_view sig_msg,
        const Signature& sig,
        const Ed25519PK& pubkey,
        const std::optional<SubkeyTag>& subkey_tag);

}  // namespace spns::hive
