#
# Apple Push Notification service
#
# This runs in its own process and is fed notifications by HiveMind when incoming messages arrive.
#
# When we receive a notification we send along a payload as follows:
#
# Bundles and encrypts all the details of a notification message.  So as to not to double-base64, we
# split this into a bt-encoded list of one or two components:
# - json object containing message metadata:
#   { '@': session_id, '#': msghash, 'n': namespace, 'l': actual_message_size, 'B': true_if_too_big }
# - raw message (bytes)
#
# The second element (the message) is omitted if not available (e.g. because the user subscription
# didn't want data included) or if too big.  In the former case, neither `l` nor `B` will be
# included in the metadata; in the latter case both will be in included `B`.
#
# We then take that bencoded list, pad it with trailing \0s to the next 256 byte multiple, then
# encrypt the whole thing using the `enc_key` provided at registration and a random nonce, stick the
# 24-byte random nonce on the beginning of the ciphertext, then finally base64 encode the whole
# thing and set it in the `enc_payload` field.  So you get:
#
# {
#   'aps': BASE_APS (see below),
#   'enc_payload': B64(NONCE+ENCRYPTED(l123:{...json...}456:...msg...e)),
#   'spns': 1
# }
#
# (The `spns` key will be incremented whenever something we send changes).
#
# To allow for future compatibility, the bencoded list may contain more than 2 elements (any futher
# elements should be ignored).
#

from aioapns import APNs, NotificationRequest, PushType, PRIORITY_HIGH
import asyncio
from threading import Lock
from datetime import timedelta
import json
import signal

from .. import config
from ..config import logger
from ..core import SUBSCRIBE
from .util import encrypt_payload, derive_notifier_key, warn_on_except, NotifyStats

import oxenc
from oxenmq import OxenMQ, Message, Address, AuthLevel

omq = None
hivemind = None
loop = None
apns = None

# The content of the `"aps"` field
BASE_APS = {
    "alert": {"title": "Session", "body": "You've got a new message"},
    "badge": 1,
    "sound": "default",
    "mutable-content": 1,
    "category": "SECRET",
}

# Whenever we add/change fields we increment this so that a Session client could figure out what it
# is speaking to:
SPNS_APNS_VERSION = 1

# If our JSON payload hits 4kiB then Apple will reject it, so we limit ourselves to this size
# *before* encryption + base64 encoding.  If the source message exceeds this, we send an alternative
# "too big" response instead.
MAX_MSG_SIZE = 2500


stats = NotifyStats()


@warn_on_except
def validate(msg: Message):
    parts = msg.data()
    if len(parts) != 2 or parts[0] != b"apns":
        logger.warning("Internal error: invalid input to notifier.validate")
        msg.reply(str(SUBSCRIBE.ERROR.value), "Internal error")
        return

    try:
        data = json.loads(parts[1])

        # We require just the device token, passed as `token`:
        token = data["token"]
        if not token or len(token) != 64:
            raise ValueError(f"Invalid token: expected length-64 device token, not {len(token)}")
        msg.reply("0", token)
    except KeyError as e:
        msg.reply(str(SUBSCRIBE.BAD_INPUT.value), f"Error: missing required key {e}")
    except Exception as e:
        msg.reply(str(SUBSCRIBE.ERROR.value), str(e))


def make_notifier(request: NotificationRequest):
    async def apns_notify():
        global apns
        max_retries = config.NOTIFY["apns"].get("retries", 0)
        retry_sleep = config.NOTIFY["apns"].get("retry_interval", 10)
        retries = max_retries
        while True:
            response = await apns.send_notification(request)
            if response.is_successful:
                with stats.lock:
                    stats.notifies += 1
                    if retries < max_retries:
                        stats.notify_retries += 1
                return
            if retries > 0:
                retries -= 1
                await asyncio.sleep(retry_sleep)
            else:
                with stats.lock:
                    stats.failures += 1
                logger.warning(
                    f"Failed to send notification: {response.status} ({response.description}); giving up after {max_retries} retries"
                )

    return apns_notify


@warn_on_except
def push_notification(msg: Message):
    data = oxenc.bt_deserialize(msg.data()[0])

    enc_payload = encrypt_notify_payload(data, max_msg_size=MAX_MSG_SIZE)

    device_token = data[b"&"]  # unique service id, as we returned from validate

    request = NotificationRequest(
        device_token=device_token,
        message={
            "aps": BASE_APS,
            "spns": SPNS_APNS_VERSION,
            "enc_payload": oxenc.to_base64(enc_payload),
        },
        priority=PRIORITY_HIGH,
        push_type=PushType.ALERT,
    )

    global loop
    asyncio.run_coroutine_threadsafe(make_notifier(request), loop)


@warn_on_except
def report_stats():
    global stats, omq, hivemind
    omq.send(hivemind, "admin.service_stats", "apns", oxenc.bt_serialize(stats.collect()))


def run():
    """Runs the asyncio event loop, forever."""

    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key(__name__)

    global omq, hivemind, loop, apns

    logger.info("Starting apns notifier")

    try:
        omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

        cat = omq.add_category("notifier", AuthLevel.basic)
        cat.add_request_command("validate", validate)
        cat.add_command("push", push_notification)

        omq.add_timer(report_stats, timedelta(seconds=5))

        omq.start()

        hivemind = omq.connect_remote(
            Address(config.config.hivemind_sock),
            auth_level=AuthLevel.basic,
            ephemeral_routing_id=False,
        )

        conf = config.NOTIFY["apns"]
        apns = APNs(
            client_cert=conf["cert_file"],
            use_sandbox=bool(config.looks_true(conf.get("use_sandbox"))),
            topic=conf["identifier"],
        )

        omq.send(hivemind, "admin.register_service", "apns")

    except Exception as e:
        logger.critical(f"Failed to start up APNS notifier: {e}")

    logger.info("apns notifier started")

    def sig_die(signum, frame):
        raise OSError(f"Caught signal {signal.Signals(signum).name}")

    try:
        loop = asyncio.new_event_loop()

        signal.signal(signal.SIGHUP, sig_die)
        signal.signal(signal.SIGINT, sig_die)

        loop.run_forever()
    except Exception as e:
        logger.critical(f"APNS run loop failed: {e}")
    finally:
        loop.close()

    logger.info("apns notifier shut down")

    omq.disconnect(hivemind)
    hivemind = None
    omq = None
    loop = None
