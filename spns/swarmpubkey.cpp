#include "swarmpubkey.hpp"

#include <oxenc/endian.h>
#include <sodium/crypto_sign_ed25519.h>

namespace spns {

static_assert(is_bytes<AccountID>);
static_assert(!is_bytes<std::array<unsigned char, 32>>);

static uint64_t calc_swarm_space(const AccountID& id) {
    uint64_t res = 0;
    for (int i = 1; i < 33; i += 8)
        res ^= oxenc::load_big_to_host<uint64_t>(id.data() + i);
    return res;
}

SwarmPubkey::SwarmPubkey(AccountID account_id, std::optional<Ed25519PK> ed, bool _skip_validation) :
        id{std::move(account_id)}, swarm_space{calc_swarm_space(id)} {

    if (ed) {
        if (id.front() != std::byte{0x05})
            throw std::invalid_argument{
                    "session_ed25519 may only be used with 05-prefixed session IDs"};
        ed25519 = std::move(*ed);
        session_ed = true;
        if (!_skip_validation) {
            AccountID derived_pk;
            derived_pk[0] = std::byte{0x05};
            int rc = crypto_sign_ed25519_pk_to_curve25519(
                    static_cast<unsigned char*>(derived_pk) + 1, ed25519);
            if (rc != 0)
                throw std::invalid_argument{"Failed to convert session_ed25519 to x25519 pubkey"};
            if (derived_pk != id)
                throw std::invalid_argument{
                        "account_id/session_ed25519 mismatch: session_ed25519 does not convert to "
                        "given account_id"};
        }
    } else {
        std::memcpy(ed25519.data(), id.data() + 1, 32);
    }
    swarm = INVALID_SWARM_ID;
}

bool SwarmPubkey::update_swarm(const std::vector<uint64_t>& swarm_ids) const {

    uint64_t closest;
    if (swarm_ids.size() == 0)
        closest = INVALID_SWARM_ID;
    else if (swarm_ids.size() == 1)
        closest = swarm_ids.front();
    else {
        // Adapted from oxen-storage-server:

        // Find the right boundary, i.e. first swarm with swarm_id >= res
        auto right_it = std::lower_bound(swarm_ids.begin(), swarm_ids.end(), swarm_space);
        if (right_it == swarm_ids.end())
            // res is > the top swarm_id, meaning it is big and in the wrapping space between
            // last and first elements.
            right_it = swarm_ids.begin();

        // Our "left" is the one just before that (with wraparound, if right is the first swarm)
        auto left_it = std::prev(right_it == swarm_ids.begin() ? swarm_ids.end() : right_it);

        uint64_t dright = *right_it - swarm_space;
        uint64_t dleft = swarm_space - *left_it;

        closest = dright < dleft ? *right_it : *left_it;
    }

    if (closest != swarm) {
        swarm = closest;
        return true;
    }
    return false;
}

}  // namespace spns
