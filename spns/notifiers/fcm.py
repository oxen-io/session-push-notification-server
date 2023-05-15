# Google Firebase push notification server, using aiofcm for higher performance than the stock
# bloated Google Firebase Python API.
#

from aiofcm import FCM, Message, PRIORITY_HIGH
import asyncio

from .. import config
from ..config import logger
from ..core import SUBSCRIBE
from .util import encrypt_payload, warn_on_except

from oxenc import bt_serialize, bt_deserialize, to_base64

omq = None
hivemind = None
loop = None
fcm = None

# Whenever we add/change fields we increment this so that a Session client could figure out what it
# is speaking to:
SPNS_FCM_VERSION = 1

# If our JSON payload hits 4000 bytes then Google will reject it, so we limit ourselves to this size
# *before* encryption + base64 encoding.  If the source message exceeds this, we send an alternative
# "too big" response in the metadata instead of including the message.
MAX_MSG_SIZE = 2500


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


def make_notifier(msg: Message):
    async def fcm_notify():
        global fcm
        max_retries = config.NOTIFY["firebase"].get("retries", 0)
        retry_sleep = config.NOTIFY["firebase"].get("retry_interval", 10)
        retries = max_retries
        while True:
            response = await fcm.send_message(msg)
            if response.is_successful:
                return
            if retries > 0:
                retries -= 1
                await asyncio.sleep(retry_sleep)
            else:
                logger.warning(
                    f"Failed to send notification: {response.status} ({response.description}); giving up after {max_retries} retries"
                )

    return fcm_notify


@warn_on_except
def push_notification(msg: Message):
    data = bt_deserialize(msg[0])

    enc_payload = encrypt_notify_payload(data, max_msg_size=MAX_MSG_SIZE)

    device_token = data["&"]  # unique service id, as we returned from validate

    msg = Message(
        device_token=device_token, data={"enc_payload": enc_payload}, priority=PRIORITY_HIGH
    )

    global loop
    asyncio.run_coroutine_threadsafe(make_notifier(msg), loop)


def run():
    """Runs the asyncio event loop, forever."""

    # These do not and *should* not match hivemind or any other notifier: that is, each notifier
    # needs its own unique keypair.  We do, however, want it to persist so that we can
    # restart/reconnect and receive messages sent while we where restarting.
    key = derive_notifier_key(__name__)

    global omq, hivemind, firebase

    omq = OxenMQ(pubkey=key.public_key.encode(), privkey=key.encode())

    cat = omq.add_category("notifier", AuthLevel.basic)
    cat.add_request_command("validate", validate)
    cat.add_command("push", push_notification)

    omq.start()

    hivemind = omq.connect_remote(
        Address(config.config.hivemind_sock), auth_level=AuthLevel.basic, ephemeral_routing_id=False
    )

    conf = config.NOTIFY["firebase"]
    fcm = FCM()  # FIXME?

    omq.send(hivemind, "admin.register_service", "firebase")

    try:
        loop.run_forever()
    finally:
        loop.close()

    omq.disconnect(hivemind)
    hivemind = None
    omq = None
    loop = None
