from . import web
from .web import app
from .core import SUBSCRIBE
from flask import request, jsonify, Response


@app.post("/subscribe")
def subscribe():
    """
    Register for push notifications.

    The body of this request is a JSON object This expects JSON input of:

    {
        "pubkey": "05123...",
        "session_ed25519": "abc123...",
        "subaccount": "def789...",
        "subaccount_sig": "aGVsbG9...",
        "namespaces": [-400,0,1,2,17],
        "data": true,
        "sig_ts": 1677520760,
        "signature": "f8efdd120007...",
        "service": "apns",
        "service_info": { ... },
        "enc_key": "abcdef..."
    }

    or an array of such JSON objects (to submit multiple subscriptions at once).

    where keys are as follows (note that all bytes values shown above in hex can be passed either as
    hex or base64):

    - pubkey -- the 33-byte account being subscribed to; typically a session ID.
    - session_ed25519 -- when the `pubkey` value starts with 05 (i.e. a session ID) this is the
      underlying ed25519 32-byte pubkey associated with the session ID.  When not 05, this field
      should not be provided.
    - subaccount -- 36-byte swarm authentication subccount tag provided by an account owner
    - subaccount_sig -- 64-byte Ed25519 signature of the subaccount tag signed by the account owner
    - namespaces -- list of integer namespace (-32768 through 32767).  These must be sorted in
      ascending order.
    - data -- if provided and true then notifications will include the body of the message (as long
      as it isn't too large); if false then the body will not be included in notifications.
    - sig_ts -- the signature unix timestamp (seconds, not ms); see below.
    - signature -- the 64-byte Ed25519 signature; see below.
    - service -- the string identifying the notification service, such as "apns" or "firebase".
    - service_info -- dict of service-specific data; typically this includes a "token" field with a
      device-specific token, but different services may have different input requirements.
    - enc_key -- 32-byte encryption key; notification payloads sent to the device will be encrypted
      with XChaCha20-Poly1305 using this key.

    Notification subscriptions are unique per pubkey/service/service_token which means that
    re-subscribing with the same pubkey/service/token renews (or updates, if there are changes in
    other parameters such as the namespaces) an existing subscription.

    Signatures:

    The signature data collected and stored here is used by the PN server to subscribe to the swarms
    for the given accounts; the specific rules are governed by the storage server, but in general:

    - a signature must have been produced (via the timestamp) within the past 14 days.  It is
      recommended that clients generate a new signature whenever they re-subscribe, and that
      re-subscriptions happen more frequently than once every 14 days.

    - a signature is signed using the account's Ed25519 private key (or delegated Ed25519
      subaccount, if using subaccount authentication), and signs the value:

      "MONITOR" || HEX(ACCOUNT) || SIG_TS || DATA01 || NS[0] || "," || ... || "," || NS[n]

      where SIG_TS is the `sig_ts` value as a base-10 string; DATA01 is either "0" or "1" depending
      on whether the subscription wants message data included; and the trailing NS[i] values are a
      comma-delimited list of namespaces that should be subscribed to, in the same sorted order as
      the `namespaces` parameter.

    Returns json such as:

    { "success": true, "added": true }

    on acceptance of a new registration, or:

    { "success": true, "updated": true }

    on renewal/update of an existing device registration.

    On error returns:

    { "error": CODE, "message": "some error description" }

    where CODE is one of the integer values of the spns/hive/subscription.hpp SUBSCRIBE enum.

    If called with an array of subscriptions then an array of such json objects is returned, where
    return value [n] is the response for request [n].
    """

    clen = request.content_length
    if clen is None:
        return jsonify(
            {"error": SUBSCRIBE.BAD_INPUT.value, "message": "Invalid request: request body missing"}
        )
    if request.content_length > 100_000:
        return jsonify(
            {"error": SUBSCRIBE.BAD_INPUT.value, "message": "Invalid request: request too large"}
        )

    try:
        resp = web.omq.request_future(web.hivemind, "push.subscribe", request.get_data()).get()
    except TimeoutError as e:
        app.logger.warning(f"Timeout proxying subscription to hivemind backend ({e})")
        return jsonify(
            {
                "error": SUBSCRIBE.SERVICE_TIMEOUT.value,
                "message": "Timeout waiting for push notification backend",
            }
        )
    except Exception as e:
        app.logger.warning(f"Error proxying subscription to hivemind backend: {e}")
        return jsonify(
            {
                "error": SUBSCRIBE.ERROR.value,
                "message": "An error occured while processing your request",
            }
        )

    return Response(resp[0], mimetype="application/json")


@app.post("/unsubscribe")
def unsubscribe():
    """
    Removes a device registration from push notifications.

    The request should be json with a body with a subset of the /subscribe parameters:

    {
        "pubkey": "05123...",
        "session_ed25519": "abc123...",
        "subaccount": "def789...",
        "subaccount_sig": "aGVsbG9...",
        "sig_ts": 1677520760,
        "signature": "f8efdd120007...",
        "service": "apns",
        "service_info": { ... }
    }

    (or a list of such elements).

    The signature here is over the value:

      "UNSUBSCRIBE" || HEX(ACCOUNT) || SIG_TS

    and SIG_TS must be within 24 hours of the current time.

    On success returns:

    { "success": true, "removed": true }

    if a registration for the given pubkey/service info was found and removed; or:

    { "success": true, "removed": false }

    if the request was accepted (i.e. signature validated) but the registration did not exist (e.g.
    was already removed).

    On failure returns:

    { "error": INT, "message": "some error message" }

    where INT is one of the error integers from spns/hive/subscription.cpp's SUBSCRIBE enum.

    If this request is invoked with a list of unsubscribe requests then a list of such error objects
    is returned, one for each unsubscribe request.
    """

    clen = request.content_length
    if clen is None:
        return jsonify(
            {"error": SUBSCRIBE.BAD_INPUT.value, "message": "Invalid request: request body missing"}
        )
    if request.content_length > 100_000:
        return jsonify(
            {"error": SUBSCRIBE.BAD_INPUT.value, "message": "Invalid request: request too large"}
        )

    try:
        resp = web.omq.request_future(web.hivemind, "push.unsubscribe", request.get_data()).get()
    except TimeoutError as e:
        app.logger.warning(f"Timeout proxying subscription to hivemind backend ({e})")
        return jsonify(
            {
                "error": SUBSCRIBE.SERVICE_TIMEOUT.value,
                "message": "Timeout waiting for push notification backend",
            }
        )
    except Exception:
        app.logger.warning(f"Error proxying subscription to hivemind backend: {e}")
        return jsonify(
            {
                "error": SUBSCRIBE.ERROR.value,
                "message": "An error occured while processing your request",
            }
        )

    return Response(resp[0], mimetype="application/json")
