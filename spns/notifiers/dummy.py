from oxenmq import OxenMQ, AuthLevel, Message, Address
import oxenc
from nacl.hash import blake2b as blake2b_oneshot
from nacl.encoding import HexEncoder
from threading import Lock
import json
import traceback
import time
from ..hive.subscription import SUBSCRIBE
from .util import derive_notifier_key
from .. import config
from ..config import logger
from ..utils import warn_on_except
from datetime import timedelta

omq = None
hivemind = None

stats_lock = Lock()
notifies = 0  # Total successful notifications
failures = 0  # Failed notifications (i.e. neither first attempt nor retries worked)


@warn_on_except
def validate(msg: Message):
    parts = msg.data()
    logger.info(f"validate! {parts}")
    if len(parts) != 2 or parts[0] != b"dummy":
        logger.warning("Internal error: invalid input to notifier.validate")
        msg.reply(str(SUBSCRIBE.ERROR.value), "Internal error")
        return

    try:
        logger.debug("validate 1")
        data = json.loads(parts[1])

        # We can validate/require whatever data we want:
        foo = data["foo"]
        logger.warning("foo1")
        logger.debug("validate 3")
        if not foo or not foo.startswith("TEST-"):
            logger.debug("validate 4")
            raise ValueError("Invalid input: foo must start with TEST-")
        logger.warning("foo2")
        bar = data["bar"]
        if not isinstance(bar, int):
            logger.debug("validate 5")
            raise ValueError("Invalid input: bar must be an integer")

        logger.warning("foo3")
        # This could just be some magic device id provided directly, or could be some id we can
        # deterministically generate, e.g. with a hash like this:
        unique_id = blake2b_oneshot(
            f"{bar}_{foo}".encode(), digest_size=48, key=b"TestNotifier", encoder=HexEncoder
        )
        logger.warning("foo4")

        msg.reply("0", unique_id, oxenc.bt_serialize({"foo": foo, "bar": bar}))
    except KeyError as e:
        msg.reply(str(SUBSCRIBE.BAD_INPUT.value), f"Error: missing required service_info key {e}")
    except Exception as e:
        msg.reply(str(SUBSCRIBE.ERROR.value), str(e))


@warn_on_except
def push_notification(msg: Message):
    data = oxenc.bt_deserialize(msg.data()[0])
    logger.error(
        f"Dummy notifier received push for {data[b'&']}, enc_key {data[b'^']}, message hash {data[b'#']} ({len(data[b'~'])}B) for account {data[b'@']}/{data[b'n']}"
    )
    global stats_lock, notifies
    with stats_lock:
        notifies += 1


@warn_on_except
def report_stats():
    global omq, hivemind, stats_lock, notifies, failures
    logger.warning(f"dummy reporting stats {hivemind}")
    with stats_lock:
        report = {"notifies": notifies, "failures": failures}
        notifies, failures = 0, 0

    omq.send(hivemind, "admin.service_stats", "dummy", oxenc.bt_serialize(report))


def connect_to_hivemind():
    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key(__name__)

    global omq, hivemind

    omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

    cat = omq.add_category("notifier", AuthLevel.basic)
    cat.add_request_command("validate", validate)
    cat.add_command("push", push_notification)

    omq.add_timer(report_stats, timedelta(seconds=1))

    omq.start()

    hivemind = omq.connect_remote(
        Address(config.HIVEMIND_SOCK), auth_level=AuthLevel.basic, ephemeral_routing_id=False
    )

    omq.send(hivemind, "admin.register_service", "dummy")


def disconnect():
    global omq, hivemind
    if omq:
        omq.disconnect(hivemind)
        hivemind = None
        omq = None


def run():
    """Entry point when configured to run as a mule"""
    try:
        logger.info("dummy test notifier starting up")
        connect_to_hivemind()
        logger.info("dummy test notifier connected to hivemind")
        while True:
            time.sleep(1)

    except Exception:
        logger.error(f"dummy test notifier mule died via exception:\n{traceback.format_exc()}")
