#pragma once

#include <cstdint>
#include <limits>

#include "bytes.hpp"

namespace spns {

inline constexpr uint64_t INVALID_SWARM_ID = std::numeric_limits<uint64_t>::max();

struct SwarmPubkey {
    AccountID id;
    Ed25519PK ed25519;
    bool session_ed = false;  // True if the ed25519 is different from the account id (i.e. for
                              // Session X25519 pubkey accounts).
    uint64_t swarm_space;
    mutable uint64_t swarm;

    bool operator==(const SwarmPubkey& other) const { return id == other.id; }
    bool operator!=(const SwarmPubkey& other) const { return !(*this == other); }

    SwarmPubkey(AccountID account_id, std::optional<Ed25519PK> ed, bool _skip_validation = false);

    bool update_swarm(const std::vector<uint64_t>& swarm_ids) const;
};

}  // namespace spns

namespace std {

template <>
struct hash<spns::SwarmPubkey> {
    size_t operator()(const spns::SwarmPubkey& x) const {
        // A random chunk of the inside of the pubkey is already a good hash without
        // needing to otherwise hash the byte string
        static_assert(
                alignof(spns::SwarmPubkey) >= alignof(size_t) &&
                offsetof(spns::SwarmPubkey, id) % sizeof(size_t) == 0);
        return *reinterpret_cast<const size_t*>(x.id.data() + 16);
    }
};

}  // namespace std
