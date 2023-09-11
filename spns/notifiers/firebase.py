# Google Firebase push notification server

from .. import config
from ..config import logger
from ..core import SUBSCRIBE
from .util import encrypt_notify_payload, derive_notifier_key, warn_on_except, NotifyStats

import firebase_admin
from firebase_admin import messaging
from firebase_admin.exceptions import *

import oxenc
from oxenmq import OxenMQ, Message, Address, AuthLevel

import datetime
import time
import json
import signal
import systemd.daemon
from threading import Lock

omq = None
hivemind = None
firebase_app = None

notify_queue = []
queue_lock = Lock()
queue_timer = None

# Whenever we add/change fields we increment this so that a Session client could figure out what it
# is speaking to:
SPNS_FIREBASE_VERSION = 1

# If our JSON payload hits 4000 bytes then Google will reject it, so we limit ourselves to this size
# *before* encryption + base64 encoding.  If the source message exceeds this, we send an alternative
# "too big" response in the metadata instead of including the message.
MAX_MSG_SIZE = 2500

# Firebase max simultaneous notifications:
MAX_NOTIFIES = 500


stats = NotifyStats()


@warn_on_except
def validate(msg: Message):
    parts = msg.data()
    if len(parts) != 2 or parts[0] != b"firebase":
        logger.warning("Internal error: invalid input to notifier.validate")
        msg.reply(str(SUBSCRIBE.ERROR.value), "Internal error")
        return

    try:
        data = json.loads(parts[1])

        # We require just the device token, passed as `token`:
        token = data["token"]
        if not token:
            raise ValueError(f"Invalid firebase device token")
        msg.reply("0", token)
    except KeyError as e:
        msg.reply(str(SUBSCRIBE.BAD_INPUT.value), f"Error: missing required key {e}")
    except Exception as e:
        msg.reply(str(SUBSCRIBE.ERROR.value), str(e))


@warn_on_except
def push_notification(msg: Message):
    data = oxenc.bt_deserialize(msg.data()[0])

    enc_payload = encrypt_notify_payload(data, max_msg_size=MAX_MSG_SIZE)

    device_token = data[b"&"].decode()  # unique service id, as we returned from validate

    msg = messaging.Message(
        data={"enc_payload": oxenc.to_base64(enc_payload), "spns": f"{SPNS_FIREBASE_VERSION}"},
        token=device_token,
        android=messaging.AndroidConfig(priority="high"),
    )

    global notify_queue, queue_lock
    with queue_lock:
        notify_queue.append(msg)


@warn_on_except
def send_pending():
    global notify_queue, queue_lock, firebase_app, stats
    with queue_lock:
        queue, notify_queue = notify_queue, []

    i = 0
    while i < len(queue):
        results = messaging.send_all(messages=queue[i : i + MAX_NOTIFIES], app=firebase_app)
        with stats.lock:
            stats.notifies += min(len(queue) - i, MAX_NOTIFIES)

        # FIXME: process/reschedule failures?

        i += MAX_NOTIFIES


@warn_on_except
def ping():
    """Makes sure we are registered and reports updated stats to hivemind; called every few seconds"""
    global omq, hivemind, stats
    omq.send(hivemind, "admin.register_service", "firebase")
    omq.send(hivemind, "admin.service_stats", "firebase", oxenc.bt_serialize(stats.collect()))
    systemd.daemon.notify(
        f"WATCHDOG=1\nSTATUS=Running; {stats.total_notifies} notifications, "
        f"{stats.total_retries} retries, {stats.total_failures} failures"
    )


def start():
    """Starts up the firebase listener."""

    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key("firebase")

    global omq, hivemind, firebase, queue_timer

    omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

    cat = omq.add_category("notifier", AuthLevel.basic)
    cat.add_request_command("validate", validate)
    cat.add_command("push", push_notification)

    omq.add_timer(ping, datetime.timedelta(seconds=5))

    conf = config.NOTIFY["firebase"]
    queue_timer = omq.add_timer(
        send_pending,
        datetime.timedelta(seconds=float(conf["notify_interval"])),
        thread=omq.add_tagged_thread("firebasenotify"),
    )

    omq.start()

    hivemind = omq.connect_remote(
        Address(config.config.hivemind_sock), auth_level=AuthLevel.basic, ephemeral_routing_id=False
    )

    firebase_app = firebase_admin.initialize_app(
        firebase_admin.credentials.Certificate(conf["token_file"])
    )

    omq.send(hivemind, "admin.register_service", "firebase")


def disconnect(flush_pending=True):
    global omq, hivemind, queue_timer
    omq.disconnect(hivemind)
    omq.cancel_timer(queue_timer)
    omq = None
    hivemind = None

    # In case we have pending incoming notifications still to process
    time.sleep(0.5)

    if flush_pending:
        send_pending()


def run(startup_delay=4.0):
    """Runs the firebase notifier, forever."""

    global omq

    if startup_delay > 0:
        time.sleep(startup_delay)

    logger.info("Starting firebase notifier")
    systemd.daemon.notify("STATUS=Initializing firebase notifier...")
    try:
        start()
    except Exception as e:
        logger.critical(f"Failed to start firebase notifier: {e}")
        raise e

    logger.info("Firebase notifier started")
    systemd.daemon.notify("READY=1\nSTATUS=Started")

    def sig_die(signum, frame):
        raise OSError(f"Caught signal {signal.Signals(signum).name}")

    try:
        signal.signal(signal.SIGHUP, sig_die)
        signal.signal(signal.SIGINT, sig_die)

        while omq is not None:
            time.sleep(3600)
    except Exception as e:
        logger.error(f"firebase notifier mule died via exception: {e}")


if __name__ == "__main__":
    run(startup_delay=0)
