from curve25519 import _curve25519
from base64 import b64decode, b64encode
import hmac
import hashlib
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from const import PRIVKEY_FILE
from Crypto.Random import get_random_bytes

IV_LENGTH = 16
NONCE_LENGTH = 12
TAG_LENGTH = 16


def is_ios_device_token(token):
    if len(token) == 64:
        return True
    return False


def make_symmetric_key(client_pubkey):
    server_privkey = ''
    if os.path.isfile(PRIVKEY_FILE):
        with open(PRIVKEY_FILE, 'r') as server_privkey_file:
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
