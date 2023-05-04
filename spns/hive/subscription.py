from __future__ import annotations

from .swarmpubkey import SwarmPubkey
from .signature import verify_storage_signature

from nacl.encoding import RawEncoder
from nacl.exceptions import BadSignatureError
import time
from typing import Optional
from enum import Enum

# How long until we expire subscriptions (relative to the signature timestamp).  This can be *at
# most* 14 days (because that's the subscription cutoff for storage server).
SIGNATURE_EXPIRY = 14 * 86400

# Maximum signature timestamp we will accept for a new subscription.  The subscription will stay
# active for the expiry defined above; this just defines how old a subscription can be and is
# generally shorter than the above.
#
# FIXME: apply this limit
SIGNATURE_TIMESTAMP_CUTOFF = SIGNATURE_EXPIRY // 2

# How much we allow an unsubscribe signature timestamp to be off before we reject it
UNSUBSCRIBE_GRACE = 86400


class SUBSCRIBE(Enum):
    OK = 0  # Success
    BAD_INPUT = (
        1  # Unparseable, invalid values, missing required arguments, etc. (details in the string)
    )
    SERVICE_NOT_AVAILABLE = 2  # The requested service name isn't currently available
    SERVICE_TIMEOUT = 3
    ERROR = 4  # There was some other error processing the subscription (details in the string)


class SubscribeError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class Subscription:
    """Stores a single subscription for an account.  This consists of the wanted namespaces, whether
    data is desired, and the signature."""

    def __init__(
        self,
        pubkey: SwarmPubkey,
        *,
        subkey_tag: Optional[bytes] = None,
        namespaces: list[int],
        want_data: bool,
        sig_ts: int,
        sig: bytes,
        enc_key: bytes,
        _skip_validation: bool = False,
    ):
        """
        Constructs a subscription.  Throws if any parameters are invalid or the signature does not
        verify.

        This is normally called via HiveMind.add_subscription.

        Parameters:
        - pubkey -- the main SwarmPubkey object containing the pubkey which this subscription
          monitors.
        - subkey_tag -- a subkey tag to use for authentication (32 bytes).  If provided then subkey
          authentication will be used/verified.
        - namespaces -- sorted list of namespaces to monitor
        - want_data -- if true, request message data in notifications
        - sig_ts -- integer unix timestamp when the signature was generated; must be less than 2
          weeks old and no more than 24h from now.
        - signature -- 64-byte signature data
        - enc_key -- 32-byte encryption key used to encrypt pushed data.
        - _skip_validation -- internal use only; skips value validation when the fields are already
          known to be valid (e.g. when loading a subscription out of the database).
        """

        if not _skip_validation:
            if not namespaces or not isinstance(namespaces, list):
                raise ValueError("Subscription: namespaces missing or empty")
            if not sig or len(sig) != 64:
                raise ValueError("Subscription: signature must be 64 bytes")
            if subkey_tag and (not isinstance(subkey_tag, bytes) or len(subkey_tag) != 32):
                raise ValueError("Subscription: invalid subkey tag: subkey tags must be 32 bytes")

            if not isinstance(namespaces, list) or not all(isinstance(n, int) for n in namespaces):
                raise ValueError("Subscription: namespaces must be a list of integers")
            for i in range(len(namespaces) - 1):
                if namespaces[i] > namespaces[i + 1]:
                    raise ValueError("Subscription: namespaces are not sorted numerically")
                if namespaces[i] == namespaces[i + 1]:
                    raise ValueError("Subscription: namespaces contains duplicates")
            if namespaces[0] < -32768 or namespaces[-1] > 32767:
                raise ValueError(
                    "Subscription: Invalid namespaces: list contains out-of-bounds namespace"
                )

            if not sig_ts:
                raise ValueError("Subscription: signature timestamp is missing")
            if not isinstance(sig_ts, int):
                raise ValueError("Subscription: sig_ts must be an integer")
            now = int(time.time())
            if sig_ts <= now - 14 * 24 * 60 * 60:
                raise ValueError("Subscription: sig_ts timestamp is too old")
            if sig_ts >= now + 24 * 60 * 60:
                raise ValueError("Subscription: sig_ts timestamp is too far in the future")

            if not enc_key:
                raise ValueError("Subscription: enc_key required")
            elif not isinstance(enc_key, bytes) or len(enc_key) != 32:
                raise ValueError("Subscription: enc_key must be 32 bytes")

            sig_msg = (
                f"MONITOR{pubkey.id.hex()}{sig_ts:d}{want_data:d}"
                + ",".join(f"{n:d}" for n in namespaces)
            ).encode()

            try:
                verify_storage_signature(
                    sig_msg=sig_msg,
                    signature=sig,
                    ed25519_pubkey=pubkey.ed25519_pubkey,
                    subkey_tag=subkey_tag,
                )
            except BadSignatureError:
                raise ValueError(f"Subscription: signature validation failed")

        self.pubkey = pubkey
        self.subkey_tag = subkey_tag
        self.namespaces = namespaces
        self.want_data = bool(want_data)
        self.sig_ts = sig_ts
        self.sig = sig
        self.enc_key = enc_key

    def is_same(self, other: Subscription):
        """Returns true if this Subscription is the same as `other`, that is:

        - Both have the same pubkey/ed25519/subkey authentication settings
        - Both have the same encryption key
        - Both have the same namespaces and want_data values

        This does *not* compare signatures: i.e. two different Subscriptions with different
        signature timestamps and signatures would return True.

        This is mainly intended to filter out effectively identical subscriptions.
        """
        return (
            self.pubkey == other.pubkey
            and self.subkey_tag == other.subkey_tag
            and self.namespaces == other.namespaces
            and self.want_data == other.want_data
            and self.enc_key == other.enc_key
        )

    def covers(self, other: Subscription):
        """
        Returns true if this Subscription is the same as or a superset of `other`, that is:

        - Both have the same pubkey/ed25519/subkey authentication settings
        - This subscription has at least all of namespaces of `other`
        - This subscription wants data if `other` wants data (but may or may not if `other` doesn't
          want data).

        This does *not* compare signatures or encryption keys: i.e. two different Subscriptions with
        different signature timestamps and signatures and different final service encryption keys
        would return True.

        This can be used to filter out subscriptions that are already implied by another
        subscription.  The intention here is that we should return true if *this* subscription will
        return all the same notifications and data needed for `other`, in which case we can just use
        `self` and not need an addition subscription from `other`.
        """
        if not (self.pubkey == other.pubkey and self.subkey_tag == other.subkey_tag):
            return False
        if other.want_data and not self.want_data:
            return False

        # Namespaces are sorted, so we can walk through sequentially, comparing heads, and skipping
        # any extras we have have in self.  We fail by either running out of self namespaces before
        # consuming all the other namespaces (which means other has some greater than self's
        # maximum), or when the head of self is greater than the head of other (which means self is
        # missing some at the beginning or in the middle).
        i, j = 0, 0
        while j < len(other.namespaces):
            if not i < len(self.namespaces):
                # Ran out of self namespaces before we consumed all the other namespaces
                return False
            if self.namespaces[i] > other.namespaces[j]:
                # Head of the self is greater, so we are missing (at least) one of other's
                return False
            elif self.namespaces[i] == other.namespaces[j]:
                # Equal, so we have it: advance both heads
                i += 1
                j += 1
            else:
                # [i] < [j], so skip to the next `i`
                i += 1

        return True

    def is_expired(self, now=None):
        if now is None:
            now = time.time()
        return self.sig_ts + SIGNATURE_EXPIRY < now

    def is_newer(self, other: Subscription):
        """Returns true if this Subscription has an equal or newer sig_ts than `other`."""
        return self.sig_ts >= other.sig_ts
