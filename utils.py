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
import pickle
from tinydb import TinyDB


def is_ios_device_token(token):
    return len(token) == 64


def make_symmetric_key(client_pubkey):
    if client_pubkey is None:
        return None

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


def onion_request_data_handler(data):
    ciphertext_length = int.from_bytes(data[:4], "little") + 4
    ciphertext = data[4:ciphertext_length]
    body_as_string = data[ciphertext_length:].decode('utf-8')
    body = json.loads(body_as_string)
    body[CIPHERTEXT] = b64encode(ciphertext)
    return body


def migrate_database_if_needed():
    db = TinyDB(DATABASE)

    def migrate(old_db_name, new_table_name, json_structure):
        db_map = None
        if os.path.isfile(old_db_name):
            with open(old_db_name, 'rb') as old_db:
                db_map = dict(pickle.load(old_db))
            old_db.close()
        if db_map is not None and len(db_map) > 0:
            for key, value in db_map.items():
                item = {}
                for key_name, value_name in json_structure.items():
                    item[key_name] = key
                    item[value_name] = list(value)
                db.table(new_table_name).insert(item)
            os.remove(old_db_name)

    migrate(PUBKEY_TOKEN_DB_V2, PUBKEY_TOKEN_TABLE, {PUBKEY: TOKEN})
    migrate(CLOSED_GROUP_DB, CLOSED_GROUP_TABLE, {CLOSED_GROUP: MEMBERS})
