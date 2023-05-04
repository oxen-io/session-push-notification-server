from oxenc import from_base64

from .config import logger


def decode_hex_or_b64(data: bytes, size: int):
    """
    Decodes hex or base64-encoded input of a binary value of size `size`.  Returns None if data is
    None; otherwise the bytes value, if parsing is successful.  Throws on invalid data.

    (Size is required because many hex strings are valid base64 and vice versa.)
    """
    if data is None:
        return None

    if len(data) == size * 2:
        return bytes.fromhex(data)

    b64_size = (size + 2) // 3 * 4  # bytes*4/3, rounded up to the next multiple of 4.
    b64_unpadded = (size * 4 + 2) // 3

    if b64_unpadded <= len(data) <= b64_size:
        decoded = from_base64(data)
        if len(decoded) == size:  # Might not equal our target size because of padding
            return decoded

    raise ValueError("Invalid value: could not decode as hex or base64")


def warn_on_except(f):
    def wrapper(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Exception in {f.__name__}: {e}")

    return wrapper
