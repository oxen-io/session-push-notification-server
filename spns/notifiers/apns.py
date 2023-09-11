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

import aioapns
import aioapns.logging
import asyncio
from threading import Lock
from datetime import timedelta
import time
import json
import signal
import systemd.daemon
import coloredlogs

from .. import config
from ..config import logger
from ..core import SUBSCRIBE
from .util import encrypt_notify_payload, derive_notifier_key, warn_on_except, NotifyStats

import oxenc
from oxenmq import OxenMQ, Message, Address, AuthLevel

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

MAX_RETRIES = int(config.NOTIFY["apns"].get("retries", 1))
RETRY_SLEEP = float(config.NOTIFY["apns"].get("retry_interval", 5))


coloredlogs.install(
    milliseconds=True, isatty=True, level=logger.getEffectiveLevel(), logger=aioapns.logging.logger
)


class APNSHandler:
    def __init__(self, loop, key, service_name, *, sandbox=False):
        self.loop = loop
        self.stats = NotifyStats()

        self.omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

        cat = self.omq.add_category("notifier", AuthLevel.basic)
        cat.add_request_command("validate", self.validate)
        cat.add_command("push", self.push_notification)

        self.omq.add_timer(self.ping, timedelta(seconds=5))

        self.omq.start()

        self.hivemind = self.omq.connect_remote(
            Address(config.config.hivemind_sock),
            auth_level=AuthLevel.basic,
            ephemeral_routing_id=False,
        )

        self.service_name = service_name
        conf = config.NOTIFY[service_name]
        self.apns = aioapns.APNs(
            client_cert=conf["cert_file"], use_sandbox=sandbox, topic=conf["identifier"]
        )

        self.omq.send(self.hivemind, "admin.register_service", service_name)

    @warn_on_except
    def validate(self, msg: Message):
        parts = msg.data()
        if len(parts) != 2 or parts[0] != self.service_name.encode():
            logger.warning("Internal error: invalid input to notifier.validate")
            msg.reply(str(SUBSCRIBE.ERROR.value), "Internal error")
            return

        try:
            data = json.loads(parts[1])

            # We require just the device token, passed as `token`:
            token = data["token"]
            if not token or len(token) != 64:
                raise ValueError(
                    f"Invalid token: expected length-64 device token, not {len(token)}"
                )
            msg.reply("0", token)
        except KeyError as e:
            msg.reply(str(SUBSCRIBE.BAD_INPUT.value), f"Error: missing required key {e}")
        except Exception as e:
            msg.reply(str(SUBSCRIBE.ERROR.value), str(e))

    async def apns_notify(self, request: aioapns.NotificationRequest):
        retries = MAX_RETRIES
        while True:
            logger.debug("sending notification")
            response = await self.apns.send_notification(request)
            if response.is_successful:
                logger.debug("APNS notification was successful!")
                with self.stats.lock:
                    self.stats.notifies += 1
                    if retries < MAX_RETRIES:
                        self.stats.notify_retries += 1
                return
            if retries <= 0:
                with self.stats.lock:
                    self.stats.failures += 1
                logger.warning(
                    f"Failed to send notification: giving up after {MAX_RETRIES} retries"
                )
                return
            logger.critical(
                f"status: {response.status} ({type(response.status)}), desc: {response.description} ({type(response.description)})"
            )
            if response.status in (400, "400") and response.description == "BadDeviceToken":
                with self.stats.lock:
                    self.stats.failures += 1
                logger.warning(f"Failed to send notification: invalid token; not retrying")
                return
            logger.debug(f"notification failed; will retry in {RETRY_SLEEP}s")
            retries -= 1
            await asyncio.sleep(RETRY_SLEEP)

    @warn_on_except
    def push_notification(self, msg: Message):
        data = oxenc.bt_deserialize(msg.data()[0])

        enc_payload = encrypt_notify_payload(data, max_msg_size=MAX_MSG_SIZE)

        device_token = data[b"&"].decode()  # unique service id, as we returned from validate

        logger.debug(
            f"Building APNS notification request for device {device_token}, message data: {data}"
        )
        self.loop.call_soon_threadsafe(
            asyncio.ensure_future,
            self.apns_notify(
                aioapns.NotificationRequest(
                    device_token=device_token,
                    message={
                        "aps": BASE_APS,
                        "spns": SPNS_APNS_VERSION,
                        "enc_payload": oxenc.to_base64(enc_payload),
                    },
                    priority=aioapns.PRIORITY_HIGH,
                    push_type=aioapns.PushType.ALERT,
                )
            ),
        )

    @warn_on_except
    def ping(self):
        """Makes sure we are registered and reports updated stats to hivemind; called every few seconds"""
        self.omq.send(self.hivemind, "admin.register_service", "apns")
        self.omq.send(
            self.hivemind, "admin.service_stats", "apns", oxenc.bt_serialize(self.stats.collect())
        )
        systemd.daemon.notify(
            f"WATCHDOG=1\nSTATUS=Running; {self.stats.total_notifies} notifications, "
            f"{self.stats.total_retries} retries, {self.stats.total_failures} failures"
        )

    def stop(self):
        self.omq.disconnect(self.hivemind)
        self.hivemind = None
        self.omq = None


def run(startup_delay=4.0):
    """Runs the apns asyncio event loop, forever."""

    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key("apns")

    if startup_delay > 0:
        time.sleep(startup_delay)

    logger.info("Starting apns notifier")
    systemd.daemon.notify("STATUS=Initializing firebase notifier...")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        handler = APNSHandler(loop, key, "apns")

    except Exception as e:
        logger.critical(f"Failed to start up APNS notifier: {e}")
        raise e

    logger.info("apns notifier started")
    systemd.daemon.notify("READY=1\nSTATUS=Started")

    def sig_die(signum, frame):
        raise OSError(f"Caught signal {signal.Signals(signum).name}")

    try:
        signal.signal(signal.SIGHUP, sig_die)
        signal.signal(signal.SIGINT, sig_die)

        loop.run_forever()
    except Exception as e:
        logger.critical(f"APNS run loop failed: {e}")
    finally:
        loop.close()

    logger.info("apns notifier shut down")

    handler.stop()


if __name__ == "__main__":
    run(startup_delay=0)
