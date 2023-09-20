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
from threading import Lock
import time
from collections import deque

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
    ciphertext = crypto_aead_xchacha20poly1305_ietf_encrypt(
        message=msg, key=enc_key, nonce=nonce, aad=None
    )
    return nonce + ciphertext


def encrypt_notify_payload(data: dict, max_msg_size: int = 2500):
    enc_key = data[b"^"]

    metadata = {"@": data[b"@"].hex(), "#": data[b"#"].decode(), "n": data[b"n"], "t": data[b"t"], "z": data[b"z"]}
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


class NotifyStats:
    def __init__(self):
        self.lock = Lock()

        # Stats since the last report:
        self.notifies = 0  # Total successful notifications
        self.notify_retries = 0  # Successful notifications that required 1 or more retries
        self.failures = 0  # Failed notifications (i.e. neither first attempt nor retries worked)

        self.total_notifies = 0
        self.total_retries = 0
        self.total_failures = 0

        # History of recent (time, notifies) values:
        self.notify_hist = deque()

    def collect(self):
        with self.lock:
            now = time.time()
            report = {
                "+notifies": self.notifies,
                "+notify_retries": self.notify_retries,
                "+failures": self.failures,
            }

            for mins in (60, 10, 1):
                cutoff, since, summation, n = now - mins * 60, 0, 0, 0
                for i in range(len(self.notify_hist)):
                    t, notif = self.notify_hist[i]
                    if t >= cutoff:
                        if n == 0:
                            since = self.notify_hist[i - 1 if i > 0 else i][0]
                        n += 1
                        summation += notif

                if n > 0:
                    report[f"notifies_per_day.{mins}m"] = round(
                        summation / n / (now - since) * 86400
                    )

            cutoff = now - 3600
            while self.notify_hist and self.notify_hist[0][0] < cutoff:
                self.notify_hist.popleft()
            self.notify_hist.append((now, self.notifies))
            self.total_notifies += self.notifies
            self.total_retries += self.notify_retries
            self.total_failures += self.failures
            self.notifies, self.notify_retries, self.failures = 0, 0, 0

        return report
