from nacl.hash import blake2b as blake2b_oneshot
from nacl.public import PrivateKey
from nacl.encoding import RawEncoder
import nacl.utils
from nacl.bindings import (
    crypto_aead_xchacha20poly1305_ietf_encrypt,
    crypto_aead_xchacha20poly1305_ietf_NPUBBYTES,
)
from oxenc import bt_serialize, bt_deserialize, to_base64
import json

from .. import config


# Returns a notifier key derived from the main hivemind key, given a notifier name.  This ensures
# distinct, private keys for each notifier without needing to generate multiple keys.
def derive_notifier_key(name):
    return PrivateKey(
        blake2b_oneshot(
            config.PRIVKEYS["hivemind"].encode() + name.encode(),
            key=b"notifier",
            digest_size=32,
            encoder=RawEncoder,
        )
    )


def encrypt_payload(msg: bytes, enc_key: bytes):
    nonce = nacl.utils.random(crypto_aead_xchacha20poly1305_ietf_NPUBBYTES)
    ciphertext = crypto_aead_xchacha20poly1305_ietf_encrypt(message=msg, key=enc_key, nonce=nonce)
    return nonce + ciphertext


def encrypt_notify_payload(data: dict, max_msg_size: int = 2500):
    enc_key = data[b"^"]

    metadata = {"@": data[b"@"].hex(), "#": data[b"#"], "n": data[b"n"]}
    body = data.get(b"~")

    if body:
        metadata["l"] = len(body)
        if max_msg_size >= 0 and len(body) > max_msg_size:
            metadata["B"] = True
            body = None

    payload = bt_serialize([json.dumps(metadata), body] if body else [metadata])
    over = len(payload) % 256
    if over:
        payload += b"\0" * (256 - over)

    return encrypt_payload(payload, enc_key)


def warn_on_except(f):
    """
    Wrapper that catches and logs exceptions, used for endpoint wrapping where an exception just
    gets eaten by oxenmq anyway).
    """

    def wrapper(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception as e:
            config.logger.warning(f"Exception in {f.__name__}: {e}")

    return wrapper
