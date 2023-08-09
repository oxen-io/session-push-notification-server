# Huawei push notification server

from .. import config
from ..config import logger
from ..core import SUBSCRIBE
from .util import encrypt_notify_payload, derive_notifier_key, warn_on_except, NotifyStats

from hms.src import push_admin
from hms.src.push_admin import messaging as huawei_messaging

import oxenc
from oxenmq import OxenMQ, Message, Address, AuthLevel

import datetime
import time
import json
import signal
from threading import Lock

omq = None
hivemind = None
huawei_push_admin = None

notify_queue = []
queue_lock = Lock()
queue_timer = None

# Whenever we add/change fields we increment this so that a Session client could figure out what it
# is speaking to:
SPNS_HUAWEI_VERSION = 1

# If our JSON payload hits 4000 bytes then Huawei will reject it, so we limit ourselves to this size
# *before* encryption + base64 encoding.  If the source message exceeds this, we send an alternative
# "too big" response in the metadata instead of including the message.
MAX_MSG_SIZE = 2500

# Firebase max simultaneous notifications:
MAX_NOTIFIES = 500


stats = NotifyStats()


@warn_on_except
def validate(msg: Message):
    parts = msg.data()
    if len(parts) != 2 or parts[0] != b"huawei":
        logger.warning("Internal error: invalid input to notifier.validate")
        msg.reply(str(SUBSCRIBE.ERROR.value), "Internal error")
        return

    try:
        data = json.loads(parts[1])

        # We require just the device token, passed as `token`:
        token = data["token"]
        if not token:
            raise ValueError(f"Invalid huawei device token")
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

    msg = huawei_messaging.Message(
        data=json.dumps({"enc_payload": oxenc.to_base64(enc_payload), "spns": f"{SPNS_HUAWEI_VERSION}"}),
        token=[device_token],
        android=huawei_messaging.AndroidConfig(urgency=huawei_messaging.AndroidConfig.HIGH_PRIORITY),
    )

    global notify_queue, queue_lock
    with queue_lock:
        notify_queue.append(msg)


@warn_on_except
def send_pending():
    global notify_queue, queue_lock, huawei_push_admin, stats
    with queue_lock:
        queue, notify_queue = notify_queue, []

    i = 0
    while i < len(queue):
        result = huawei_messaging.send_message(queue[i])
        with stats.lock:
            stats.notifies += 1

        # FIXME: process/reschedule failures?

        i += 1


@warn_on_except
def report_stats():
    global omq, hivemind, stats
    omq.send(hivemind, "admin.service_stats", "huawei", oxenc.bt_serialize(stats.collect()))


def start():
    """Starts up the huawei push notification listener."""

    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key(__name__)

    global omq, hivemind, huawei_push_admin, queue_timer

    omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

    cat = omq.add_category("notifier", AuthLevel.basic)
    cat.add_request_command("validate", validate)
    cat.add_command("push", push_notification)

    omq.add_timer(report_stats, datetime.timedelta(seconds=5))

    conf = config.NOTIFY["huawei"]
    queue_timer = omq.add_timer(
        send_pending,
        datetime.timedelta(seconds=float(conf["notify_interval"])),
        thread=omq.add_tagged_thread("huaweinotify"),
    )

    omq.start()

    hivemind = omq.connect_remote(
        Address(config.config.hivemind_sock), auth_level=AuthLevel.basic, ephemeral_routing_id=False
    )

    huawei_push_admin = push_admin.initialize_app(
        conf["app_id"], conf["app_secret"]
    )

    omq.send(hivemind, "admin.register_service", "huawei")


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


def run():
    """Runs the firebase notifier, forever."""

    global omq
    logger.info("Starting huawei notifier")
    try:
        start()
    except Exception as e:
        logger.critical(f"Failed to start huawei notifier: {e}")
        raise e

    logger.info("Huawei notifier started")

    def sig_die(signum, frame):
        raise OSError(f"Caught signal {signal.Signals(signum).name}")

    try:
        signal.signal(signal.SIGHUP, sig_die)
        signal.signal(signal.SIGINT, sig_die)

        while omq is not None:
            time.sleep(3600)
    except Exception as e:
        logger.error(f"huawei notifier mule died via exception: {e}")
