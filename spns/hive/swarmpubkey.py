from __future__ import annotations
import nacl.bindings as sodium
from typing import Optional


class SwarmPubkey:
    """
    Class for storing a single account id (pubkey) with swarm-related functionality.
    """

    def __init__(self, account_id: bytes, session_ed25519: Optional[bytes] = None):
        """
        Constructs a SwarmPubkey.

        - `account_id` is required; it specifies (in 33 bytes) the account ID, including network
          prefix.  For non-05 prefixed IDs this is the prefix followed by the ed25519 pubkey.
        - `session_ed25519` must be provided when `account_id` is a 05-prefixed Session ID: it
          contains the 32-byte Ed25519 pubkey underlying the Session ID.  It must convert to the
          given `account_id`.  This option must be omitted (or None) when not using a 05-prefixed
          value (which are already Ed25519 pubkeys).
        """
        if not account_id or len(account_id) != 33:
            raise ValueError("invalid account_id")
        self._id = account_id
        if session_ed25519:
            if not account_id.startswith(b"\x05"):
                raise ValueError("session_ed25519 may only be used with 05-prefixed session IDs")
            if len(session_ed25519) != 32:
                raise ValueError("invalid session_ed25519")
            try:
                derived_pk = sodium.crypto_sign_ed25519_pk_to_curve25519(session_ed25519)
            except Exception:
                raise ValueError("session_ed25519 does not convert to account id")
            if derived_pk != account_id[1:]:
                raise ValueError(
                    "account_id/session_ed25519 mismatch: session_ed25519 does not convert to given account_id"
                )
            self._ed25519 = session_ed25519

        self._id = account_id
        self._ed25519 = session_ed25519 if session_ed25519 else account_id[1:]
        self.swarm_space = self._swarm_space(self._id)
        self.swarm = None

    @property
    def id(self):
        """Accesses the 33-byte account id (as bytes)"""
        return self._id

    @property
    def ed25519_pubkey(self):
        """Accesses the 32-byte Ed25519 pubkey, as bytes.  For a 05-prefixed id this is provided
        during construction (the `.id` will be 05 followed by the X25519 pubkey derived from the
        Ed25519 key); for any other id prefix this is simply the id without the leading byte."""
        return self._ed25519

    @staticmethod
    def _swarm_space(account):
        """Returns the swarm space value for the given 33-byte account id (given in bytes, not hex).
        An account's swarm is that with a swarm value closest to this."""
        assert len(account) == 33

        res = 0
        for i in range(1, 33, 8):
            res ^= int.from_bytes(account[i : i + 8], byteorder="big", signed=False)

        return res

    def update_swarm(self, swarm_ids: list[int]):
        """Takes a sorted list of swarm_ids and updates this Account's .swarm value (if necessary)
        to the closest swarm_id to this account.  Returns True if the swarm changed, False if
        not."""

        i_right, i_end = 0, len(swarm_ids)
        while i_right < i_end:
            i_mid = (i_right + i_end) // 2
            if swarm_ids[i_mid] < self.swarm_space:
                i_right = i_mid + 1
            else:
                i_end = i_mid

        # If we are passed the end then wrap around
        if i_right == len(swarm_ids):
            i_right = 0
        # The left swarm is immediately before right, but potentially with wraparound
        i_left = (i_right if i_right > 0 else len(swarm_ids)) - 1

        dright = (swarm_ids[i_right] - self.swarm_space) % (1 << 64)
        dleft = (self.swarm_space - swarm_ids[i_left]) % (1 << 64)

        sw = swarm_ids[i_right if dright < dleft else i_left]
        if sw == self.swarm:
            return False
        self.swarm = sw
        return True

    def __eq__(self, other):
        return self._id == other._id

    def __hash__(self):
        # A random chunk of the inside of the pubkey is already a good hash without needing to
        # otherwise hash the byte string
        return int.from_bytes(self._id[16:24], "little")
