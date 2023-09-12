# Documentation

- PN subscriptions go to the `/subscribe` endpoint on

    https://push.getsession.org

via a v4 onion request.  The onion req pubkey is: d7557fe563e2610de876c0ac7341b62f3c82d5eea4b62c702392ea4368f51b3b

- The JSON payload looks like this:

        {
            "pubkey": "05123...",
            "session_ed25519": "abc123...",
            "subkey_tag": "def789...",
            "namespaces": [-400,0,1,2,17],
            "data": true,
            "sig_ts": 1677520760,
            "signature": "f8efdd120007...",
            "service": "apns",
            "service_info": { "token": "xyz123..." },
            "enc_key": "abcdef..."
        }

    where keys are as follows (note that all bytes values shown above in hex can be passed either as
    hex or base64):

    - `pubkey` -- the 33-byte account being subscribed to; typically a session ID.
    - `session_ed25519` -- when the `pubkey` value starts with 05 (i.e. a session ID) this is the
      underlying ed25519 32-byte pubkey associated with the session ID.  When not 05, this field
      should not be provided.
    - `subkey_tag` -- 32-byte swarm authentication subkey; omitted (or null) when not using subkey
      auth
    - `namespaces` -- list of integer namespace (-32768 through 32767).  These must be sorted in
      ascending order.
    - `data` -- if provided and true then notifications will include the body of the message (as long
      as it isn't too large); if false then the body will not be included in notifications.
    - `sig_ts` -- the signature unix timestamp (seconds, not ms); see below.
    - `signature` -- the 64-byte Ed25519 signature; see below.
    - `service` -- the string identifying the notification service, such as "apns" or "firebase".
    - `service_info` -- dict of service-specific data; typically this includes just a "token" field
      with a device-specific token, but different services in the future may have different input
      requirements.
    - `enc_key` -- 32-byte encryption key; notification payloads sent to the device will be encrypted
      with XChaCha20-Poly1305 using this key.  Though it is permitted for this to change, it is
      recommended that the device generate this once and persist it.

    Notification subscriptions are unique per pubkey/service/service_token which means that
    re-subscribing with the same pubkey/service/token renews (or updates, if there are changes in
    other parameters such as the namespaces) an existing subscription.

    Signatures:

    The signature data collected and stored here is used by the PN server to subscribe to the swarms
    for the given account; the specific rules are governed by the storage server, but in general:

    - a signature must have been produced (via the timestamp) within the past 14 days.  It is
      recommended that clients generate a new signature whenever they re-subscribe, and that
      re-subscriptions happen more frequently than once every 14 days.

    - a signature is signed using the account's Ed25519 private key (or Ed25519 subkey, if using
      subkey authentication with a subkey_tag, for future closed group subscriptions), and signs the value:

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

    where CODE is one of the integer values of the spns/hive/subscription.hpp SUBSCRIBE enum, here:
    https://github.com/jagerman/session-push-notification-server/blob/spns-v2/spns/hive/subscription.hpp#L21


- Notifications when received now look like this (APNS):

        {
            'aps': {
                "alert": {"title": "Session", "body": "You've got a new message"},
                "badge": 1,
                "sound": "default",
                "mutable-content": 1,
                "category": "SECRET",
            },
            'enc_payload': B64(NONCE+ENCRYPTED(l123:{...json...}456:...msg...e)),
            'spns': 1
        }

  That is:
  - `aps` is some required Apple junk.
  - `spns` is a version counter, currently 1, but will be incremented if we make significant future
    changes to the notification protocol.
  - `enc_payload` is a base64-encoded value which, in decoded (binary) form, is:
    - 24 bytes of NONCE
    - however many bytes of encryption data
      - the encryption data, once decrypted, is a 1- or 2-element bencoded list, where:
        - element [0] is the notification metadata (in JSON)
        - element [1] is the message data (in bytes).

  - Notification metadata is JSON with keys (single-letter to minimize overhead in the size-limited
    push messages):

    "@" - the session ID (hex)
    "#" - the storage server message hash
    "n" - the namespace (integer)
    "l" - the byte length of the message data
    "B" - will be present and set to true if the message data was too long for inclusion in the
          notification, omitted otherwise.

    Both "l" and "B" will be omitted if the subscription opted out of data (i.e. if it passed
    `"data": false).

  - Assuming the user subscribed to message data and the data is not too long (2.5kB, since we also
    have to go through base64 encoding after encryption and still end up <4kB), the message data
    will then be included as bytes as the second element of the enc_payload bt-encoded list.

    (If the user didn't want data, or the data was too big, then the enc_payload will only have the
    metadata object).

- For Android, the subscription request uses `"service": "firebase"`, and the request data is the
  same as iOS except with the Apple-specific `"aps"` key omitted (that is: it has the `enc_payload`
  and `spns` keys, as described above).
