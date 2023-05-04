import nacl.bindings as sodium
from nacl.hash import blake2b as blake2b_oneshot
from nacl.signing import VerifyKey
from typing import Optional


def verify_storage_signature(
    sig_msg: bytes, signature: bytes, ed25519_pubkey: bytes, subkey_tag: Optional[bytes] = None
):
    """
    Verifies that the given signature is a valid signature for `sig_msg`.  Supports regular
    ed25519_pubkey signatures as well as oxen-storage-server derived subkey signatures (if
    `subkey_tag` is given).

    Throws nacl.exceptions.BadSignatureError on signature failure; ValueError on invalid input.
    """

    if len(ed25519_pubkey) != 32:
        raise ValueError("Invalid ed25519_pubkey: expected 32 bytes")

    if subkey_tag:
        if len(subkey_tag) != 32:
            raise ValueError("Invalid authentication subkey tag: expected 32 bytes")

        # H(c || A, key="OxenSSSubkey")
        verify_pubkey = blake2b_oneshot(
            subkey_tag + ed25519_pubkey, digest_size=32, key=b"OxenSSSubkey", encoder=RawEncoder
        )
        # c + H(...)
        verify_pubkey = sodium.crypto_core_ed25519_scalar_add(subkey_tag, verify_pubkey)
        # (c + H(...)) A
        verify_pubkey = sodium.crypto_scalarmult_ed25519_noclamp(verify_pubkey, ed25519_pubkey)
        verify_pubkey = VerifyKey(verify_pubkey)
    else:
        verify_pubkey = VerifyKey(ed25519_pubkey)

    verify_pubkey.verify(sig_msg, signature)  # raises BadSignatureError on error
