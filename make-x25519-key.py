#!/usr/bin/python3

import sys
from os.path import isfile
from nacl.public import PrivateKey

if len(sys.argv) != 2 or not sys.argv[1] or sys.argv[1].startswith("-"):
    print(
        f"Usage: {sys.argv[0]} FILENAME  -- generates a random x25519 key and writes it to FILENAME"
    )
    sys.exit(1)

filename = sys.argv[1]

if isfile(filename):
    print(f"Refusing to overwrite existing file {filename}")
    sys.exit(2)

x = PrivateKey.generate()
with open(filename, "w") as f:
    print(x.encode().hex(), file=f)

print(f"Generated new private key in {filename}; corresponding pubkey is:", end='\n\t')
print(x.public_key.encode().hex())
