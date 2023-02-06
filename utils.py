from curve25519 import _curve25519
from base64 import b64decode, b64encode
import hmac
import hashlib
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from const import *
from Crypto.Random import get_random_bytes
import json
from datetime import datetime
from threading import Thread
from queue import Queue
from typing import Tuple
from enum import Enum

NONCE_LENGTH = 12
TAG_LENGTH = 16


def timestamp_to_formatted_date(timestamp):
    if timestamp is None:
        return None
    date = datetime.fromtimestamp(timestamp)
    fmt = "%Y-%m-%d %H:%M:%S"
    return date.strftime(fmt)


def formatted_date_to_timestamp(date_str):
    if date_str is None:
        return None
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for fmt in formats:
        try:
            date = datetime.strptime(date_str, fmt)
            return date.timestamp()
        except ValueError:
            pass
    return None


def is_ios_device_token(token):
    return len(token) == 64


def make_symmetric_key(client_pubkey):
    if client_pubkey is None:
        return None

    server_privkey = ''
    if os.path.isfile(Environment.PRIVKEY_FILE):
        with open(Environment.PRIVKEY_FILE, 'r') as server_privkey_file:
            server_privkey = server_privkey_file.read()
        server_privkey_file.close()
    if len(server_privkey) == 0:
        return None

    server_privkey_bytes = bytes.fromhex(server_privkey)
    client_pubkey_bytes = bytes.fromhex(client_pubkey)
    salt = 'LOKI'
    shared_secret = _curve25519.make_shared(server_privkey_bytes, client_pubkey_bytes)
    return bytes.fromhex(hmac.new(salt.encode('utf-8'), shared_secret, hashlib.sha256).hexdigest())


def decrypt(ciphertext, symmetric_key):
    iv_and_ciphertext = bytearray(b64decode(ciphertext))
    nonce = iv_and_ciphertext[:NONCE_LENGTH]
    ciphertext = iv_and_ciphertext[NONCE_LENGTH:len(iv_and_ciphertext) - TAG_LENGTH]
    tag = iv_and_ciphertext[len(iv_and_ciphertext) - TAG_LENGTH:]

    decryptor = Cipher(algorithms.AES(symmetric_key), modes.GCM(nonce, bytes(tag)), default_backend()).decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def encrypt(plaintext, symmetric_key):
    nonce = get_random_bytes(NONCE_LENGTH)
    encryptor = Cipher(algorithms.AES(symmetric_key), modes.GCM(nonce), default_backend()).encryptor()
    ciphertext = encryptor.update(plaintext.encode('utf-8')) + encryptor.finalize()
    return b64encode(nonce + ciphertext + encryptor.tag).decode('utf-8')


def onion_request_data_handler(data):
    ciphertext_length = int.from_bytes(data[:4], "little") + 4
    ciphertext = data[4:ciphertext_length]
    body_as_string = data[ciphertext_length:].decode('utf-8')
    body = json.loads(body_as_string)
    body[HTTP.OnionRequest.CIPHERTEXT] = b64encode(ciphertext)
    return body


def bencode_consume_string(body: memoryview) -> Tuple[memoryview, memoryview]:
    """
    Parses a bencoded byte string from the beginning of `body`.  Returns a pair of memoryviews on
    success: the first is the string byte data; the second is the remaining data (i.e. after the
    consumed string).
    Raises ValueError on parse failure.
    """
    pos = 0
    while pos < len(body) and 0x30 <= body[pos] <= 0x39:  # 1+ digits
        pos += 1
    if pos == 0 or pos >= len(body) or body[pos] != 0x3A:  # 0x3a == ':'
        raise ValueError("Invalid string bencoding: did not find `N:` length prefix")

    strlen = int(body[0:pos])  # parse the digits as a base-10 integer
    pos += 1  # skip the colon
    if pos + strlen > len(body):
        raise ValueError("Invalid string bencoding: length exceeds buffer")
    return body[pos : pos + strlen], body[pos + strlen :]


def onion_request_v4_data_handler(junk):
    if not (junk.payload.startswith(b'l') and junk.payload.endswith(b'e')):
        raise RuntimeError("Invalid onion request body: expected bencoded list")
    belems = memoryview(junk.payload)[1:-1]
    # Metadata json; this element is always required:
    meta, belems = bencode_consume_string(belems)

    meta = json.loads(meta.tobytes())

    # Then we can have a second optional string containing the body:
    if len(belems) > 1:
        subreq_body, belems = bencode_consume_string(belems)
        if len(belems):
            raise RuntimeError("Invalid v4 onion request: found more than 2 parts")
    else:
        subreq_body = b''

    subreq_body_json = json.loads(str(subreq_body, 'utf-8'))
    subreq_body_json.update(meta)
    return subreq_body_json


class TaskQueue(Queue):

    def __init__(self, num_workers=1):
        Queue.__init__(self)
        self.num_workers = num_workers
        self.start_workers()

    def add_task(self, task, *args, **kwargs):
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def start_workers(self):
        for i in range(self.num_workers):
            t = Thread(target=self.worker)
            t.daemon = True
            t.start()

    def worker(self):
        while True:
            item, args, kwargs = self.get()
            item(*args, **kwargs)
            self.task_done()


class DeviceType(Enum):
    iOS = "ios"
    Android = "android"
    Huawei = "huawei"
    Unknown = "unknown"

    @classmethod
    def __missing__(cls, value):
        return cls(cls.Unknown)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
