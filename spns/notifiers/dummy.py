from oxenmq import OxenMQ, AuthLevel, Message, Address
import oxenc
from nacl.hash import blake2b as blake2b_oneshot
from nacl.encoding import HexEncoder
from threading import Lock
import json
import traceback
import time
import signal
import systemd.daemon
from .util import derive_notifier_key, warn_on_except
from .. import config
from ..config import logger
from ..core import SUBSCRIBE
from datetime import timedelta

omq = None
hivemind = None

stats_lock = Lock()
notifies = 0  # Total successful notifications
failures = 0  # Failed notifications (i.e. neither first attempt nor retries worked)
total_notifies = 0
total_failures = 0


@warn_on_except
def validate(msg: Message):
    parts = msg.data()
    if len(parts) != 2 or parts[0] != b"dummy":
        logger.warning("Internal error: invalid input to notifier.validate")
        msg.reply(str(SUBSCRIBE.ERROR.value), "Internal error")
        return

    try:
        data = json.loads(parts[1])

        # We can validate/require whatever data we want:
        foo = data["foo"]
        if not foo or not foo.startswith("TEST-"):
            raise ValueError("Invalid input: foo must start with TEST-")
        bar = data["bar"]
        if not isinstance(bar, int):
            raise ValueError("Invalid input: bar must be an integer")

        # This could just be some magic device id provided directly, or could be some id we can
        # deterministically generate, e.g. with a hash like this:
        unique_id = blake2b_oneshot(
            f"{bar}_{foo}".encode(), digest_size=48, key=b"TestNotifier", encoder=HexEncoder
        )

        msg.reply("0", unique_id, oxenc.bt_serialize({"foo": foo, "bar": bar}))
    except KeyError as e:
        msg.reply(str(SUBSCRIBE.BAD_INPUT.value), f"Error: missing required service_info key {e}")
    except Exception as e:
        msg.reply(str(SUBSCRIBE.ERROR.value), str(e))


@warn_on_except
def push_notification(msg: Message):
    data = oxenc.bt_deserialize(msg.data()[0])
    logger.critical(
        f"Dummy notifier received push for {data[b'&']}, enc_key {data[b'^']}, message hash {data[b'#']} ({len(data[b'~'])}B) for account {data[b'@'].hex()}/{data[b'n']}"
    )
    global stats_lock, notifies
    with stats_lock:
        notifies += 1


@warn_on_except
def ping():
    global omq, hivemind, stats_lock, notifies, failures, total_notifies, total_failures
    with stats_lock:
        report = {"+notifies": notifies, "+failures": failures}
        total_notifies += notifies
        total_failures += failures
        notifies, failures = 0, 0

    logger.debug(f"dummy re-registering and reporting stats {report}")
    omq.send(hivemind, "admin.register_service", "dummy")
    omq.send(hivemind, "admin.service_stats", "dummy", oxenc.bt_serialize(report))
    systemd.daemon.notify(
        f"WATCHDOG=1\nSTATUS=Running; {total_notifies} notifications, {total_failures} failures"
    )


def connect_to_hivemind():
    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key("dummy")

    global omq, hivemind

    omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

    cat = omq.add_category("notifier", AuthLevel.basic)
    cat.add_request_command("validate", validate)
    cat.add_command("push", push_notification)

    omq.add_timer(ping, timedelta(seconds=5))

    omq.start()

    hivemind = omq.connect_remote(
        Address(config.config.hivemind_sock), auth_level=AuthLevel.basic, ephemeral_routing_id=False
    )

    omq.send(hivemind, "admin.register_service", "dummy")


def disconnect():
    global omq, hivemind
    if omq:
        omq.disconnect(hivemind)
        hivemind = None
        omq = None


def run(startup_delay=4.0):
    """Run the dummy notifier, forever"""

    def sig_die(signum, frame):
        raise OSError(f"Caught signal {signal.Signals(signum).name}")

    if startup_delay > 0:
        time.sleep(startup_delay)

    try:
        systemd.daemon.notify("STATUS=Initializing dummy notifier...")
        logger.info("dummy test notifier starting up")
        connect_to_hivemind()
        logger.info("dummy test notifier connected to hivemind")

        signal.signal(signal.SIGHUP, sig_die)
        signal.signal(signal.SIGINT, sig_die)

        systemd.daemon.notify("READY=1\nSTATUS=Started")

        while True:
            time.sleep(1)

    except Exception:
        logger.error(f"dummy test notifier mule died via exception:\n{traceback.format_exc()}")


if __name__ == "__main__":
    run(startup_delay=0)
