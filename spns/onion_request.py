from flask import request, abort
import json
from typing import Tuple

from . import config
from .web import app

from .subrequest import make_subrequest

import pyonionreq

_junk_parser = pyonionreq.junk.Parser(
    privkey=config.PRIVKEYS["onionreq"].encode(), pubkey=config.PUBKEYS["onionreq"].encode()
)


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


def handle_v4_onionreq_plaintext(body):
    """
    Handles a decrypted v4 onion request; this injects a subrequest to process it then returns the
    result of that subrequest.  In contrast to v3, it is more efficient (particularly for binary
    input or output) and allows using endpoints that return headers or bodies with non-2xx response
    codes.

    The body of a v4 request (post-decryption) is a bencoded list containing exactly 1 or 2 byte
    strings: the first byte string contains a json object containing the request metadata which has
    three required fields:

    - "endpoint" -- the HTTP endpoint to invoke (e.g. "/room/some-room").
    - "method" -- the HTTP method (e.g. "POST", "GET")
    - "headers" -- dict of HTTP headers for the request.  Header names are case-insensitive (i.e.
      `X-Foo` and `x-FoO` are equivalent).

    Unlike v3 requests, endpoints must always start with a /.  (If a legacy endpoint "whatever"
    needs to be accessed through a v4 request for some reason then it can be accessed via the
    "/legacy/whatever" endpoint).

    If an "endpoint" contains unicode characters then it is recommended to provide it as direct
    UTF-8 values (rather than URL-encoded UTF-8).  Both approaches will work, but the X-SOGS-*
    authentication headers will always apply on the final, URL-decoded value and so avoiding
    URL-encoding in the first place will typically simplify client implementations.

    The "headers" field typically carries X-SOGS-* authentication headers as well as fields like
    Content-Type.  Note that, unlike v3 requests, the Content-Type does *not* have any default and
    should also be specified, often as `application/json`.  Unlike HTTP requests, Content-Length is
    not required and will be ignored if specified; the content-length is always determined from the
    provided body.

    The second byte string in the request, if present, is the request body in raw bytes and is
    required for POST and PUT requests and must not be provided for GET/DELETE requests.

    Bencoding details:
        A full bencode library can be used, but the format used here is deliberately meant to be as
        simple as possible to implement without a full bencode library on hand.  The format of a
        byte string is `N:` where N is a decimal number (e.g. `123:` starts a 123-byte string),
        followed by the N bytes.  A list of strings starts with `l`, contains any number of encoded
        byte strings, followed by `e`.  (Full bencode allows dicts, integers, and list/dict
        recursion, but we do not use any of that for v4 bencoded onion requests).

    For example, the request:

        GET /room/some-room
        Some-Header: 12345

    would be encoded as:

        l79:{"method":"GET","endpoint":"/room/some-room","headers":{"Some-Header":"12345"}}e

    that is: a list containing a single 79-byte string.  A POST request such as:

        POST /some/thing
        Some-Header: a

        post body here

    would be encoded as the two-string bencoded list:

        l72:{"method":"POST","endpoint":"/some/thing","headers":{"Some-Header":"a"}}14:post body heree
            ^^^^^^^^72-byte request info json^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^   ^^^^^body^^^^^

    The return value of the request is always a 2-part bencoded list where the first part contains
    response metadata and the second contains the response body.  The response metadata is a json
    object containing:
    - "code" -- the numeric HTTP response code (e.g. 200, 403); and
    - "headers" -- a json object of header names to values.  Note that, since HTTP headers are
      case-insensitive, the header names are always returned as lower-case, and we strip out the
      'content-length' header (since it is already encoded in the length of the body part).

    For example, a simple json request response might be the two parts:

    - `{"code":200,"headers":{"content-type":"application/json"}}`
    - `{"id": 123}`

    encoded as:

        l58:{"code":200,"headers":{"content-type":"application/json"}}11:{"id": 123}e

    A more complicated request, for example for a file download, might return binary content such as:

    - `{"code":200,"headers":{"content-type":"application/octet-stream","content-disposition":"attachment; filename*=UTF-8''filename.txt"}}`
    - `My file contents`

    i.e. encoded as `l132:{...the json above...}16:My file contentse`

    Error responses (e.g. a 403) are not treated specially; that is: they still have a "code" set to
    the response code and "headers" and a body part of whatever the request returned for a body).

    The final value returned from the endpoint is the encrypted bencoded bytes, and these encrypted
    bytes are returned directly to the client (i.e. no base64 encoding applied, unlike v3 requests).
    """  # noqa: E501

    try:
        if not (body.startswith(b"l") and body.endswith(b"e")):
            raise RuntimeError("Invalid onion request body: expected bencoded list")

        belems = memoryview(body)[1:-1]

        # Metadata json; this element is always required:
        meta, belems = bencode_consume_string(belems)

        meta = json.loads(meta.tobytes())

        # Then we can have a second optional string containing the body:
        if len(belems) > 1:
            subreq_body, belems = bencode_consume_string(belems)
            if len(belems):
                raise RuntimeError("Invalid v4 onion request: found more than 2 parts")
        else:
            subreq_body = b""

        method, endpoint = meta["method"], meta["endpoint"]
        if not endpoint.startswith("/"):
            raise RuntimeError("Invalid v4 onion request: endpoint must start with /")

        response, headers = make_subrequest(
            method,
            endpoint,
            headers=meta.get("headers", {}),
            body=subreq_body,
            user_reauth=True,  # Because onion requests have auth headers on the *inside*
        )

        data = response.get_data()
        app.logger.debug(
            f"Onion sub-request for {endpoint} returned {response.status_code}, {len(data)} bytes"
        )

        meta = {"code": response.status_code, "headers": headers}

    except Exception as e:
        app.logger.warning("Invalid v4 onion request: {}".format(e))
        meta = {"code": 400, "headers": {"content-type": "text/plain; charset=utf-8"}}
        data = b"Invalid v4 onion request"

    meta = json.dumps(meta).encode()
    return b"".join(
        (b"l", str(len(meta)).encode(), b":", meta, str(len(data)).encode(), b":", data, b"e")
    )


@app.post("/oxen/v4/lsrpc")
def handle_v4_onion_request():
    """
    Parse a v4 onion request.  See handle_v4_onionreq_plaintext().
    """

    # Some less-than-ideal decisions in the onion request protocol design means that we are stuck
    # dealing with parsing the request body here in the internal format that is meant for storage
    # server, but the *last* hop's decrypted, encoded data has to get shared by us (and is passed on
    # to us in its raw, encoded form).  It looks like this:
    #
    # [N][blob][json]
    #
    # where N is the size of blob (4 bytes, little endian), and json contains *both* the elements
    # that were meant for the last hop (like our host/port/protocol) *and* the elements that *we*
    # need to decrypt blob (specifically: "ephemeral_key" and, optionally, "enc_type" [which can be
    # used to use xchacha20-poly1305 encryption instead of AES-GCM]).
    #
    # The parse_junk here takes care of decoding and decrypting this according to the fields *meant
    # for us* in the json (which include things like the encryption type and ephemeral key):
    try:
        junk = _junk_parser.parse_junk(request.data)
    except RuntimeError as e:
        app.logger.warning("Failed to decrypt onion request: {}".format(e))
        abort(400)

    # On the way back out we re-encrypt via the junk parser (which uses the ephemeral key and
    # enc_type that were specified in the outer request).  We then return that encrypted binary
    # payload as-is back to the client which bounces its way through the SN path back to the client.
    response = handle_v4_onionreq_plaintext(junk.payload)
    return junk.transformReply(response)
