#
# This contains the oxenmq "Hive Mind" process that establishes connections to all the network's
# service nodes, maintaining subscriptions with them for all the users that have enabled push
# notifications and processing incoming message notifications from those SNs.
#
# The hivemind instance runs in its own process and keeps open bidirection oxenmq connections with
# the notifiers (for proxying notifications) and uwsgi processes (for receiving client subscription
# updates).
#


from __future__ import annotations
import oxenmq
from oxenc import bt_deserialize, bt_serialize, to_base32z
from datetime import timedelta
import json
import time
import resource
from threading import RLock, Condition
from typing import Union, Optional
import traceback
from psycopg_pool import ConnectionPool
from nacl.hashlib import blake2b
from nacl.exceptions import BadSignatureError
from enum import Enum
from functools import partial
from copy import deepcopy
import re

from . import config
from .config import logger
from .hive.snode import SNode
from .hive.subscription import (
    Subscription,
    SUBSCRIBE,
    SubscribeError,
    SIGNATURE_EXPIRY,
    UNSUBSCRIBE_GRACE,
)
from .hive.swarmpubkey import SwarmPubkey
from .utils import decode_hex_or_b64, warn_on_except
from .hive.signature import verify_storage_signature


INVALID_SWARM_ID = 2**64 - 1

# Parameters for our request to get service node data:
_get_sns_params = json.dumps(
    {
        "active_only": True,
        "fields": {
            x: True
            for x in (
                "pubkey_x25519",
                "public_ip",
                "storage_lmq_port",
                "swarm_id",
                "block_hash",
                "height",
            )
        },
    }
).encode()

db_pool = ConnectionPool(config.DB_URL)


def extract_hex_or_b64(args, key, length, *, required=True):
    val = args.get(key)
    if val is None or val == "":
        if not required:
            return None
        raise ValueError(f"Missing parameter: {key}")

    if not isinstance(val, str):
        raise ValueError(f"Invalid parameter {key}: value must be a string")

    try:
        return decode_hex_or_b64(val, length)
    except Exception as e:
        raise ValueError(f"Invalid parameter {key}: {e}")


class HiveMind:
    """
    This class is responsible for connecting to remote storage servers and subscribing to message
    updates.
    """

    def __init__(self):
        """
        Constructs the long-lived HiveMind object that manages connections and subscriptions to
        service node for message monitoring.
        """

        nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
        if nofile[0] and nofile[0] < 10000 and nofile[0] < nofile[1]:
            newlim = min(10000, nofile[1])
            logger.warning(f"NOFILE limit is only {nofile[0]}; increasing to {newlim}")
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (newlim, nofile[1]))
            except ValueError as e:
                logger.error(f"Failed to increase fd limit: {e}; connections may fail!")

        # Our thread lock: this must get locked and held by the various entry points into the hive
        # mind code (public methods, oxenmq callbacks).
        self.lock = RLock()

        oxend_addr = oxenmq.Address(config.OXEND_RPC)

        logger.info("Configure hivemind oxenmq instance")
        self.omq = oxenmq.OxenMQ(
            pubkey=config.PUBKEYS["hivemind"].encode(), privkey=config.PRIVKEYS["hivemind"].encode()
        )
        self.omq.max_sockets = 50000
        self.omq.max_message_size = 10 * 1024 * 1024
        self.omq.set_general_threads(4)
        self.omq.set_reply_threads(0)
        self.omq.set_batched_threads(0)

        # We listen on a local socket for connections from other local services (web frontend,
        # notification services).
        def allow_local_conn(addr, pk, sn):
            logger.info(f"Incoming local sock connection from {addr}")
            return oxenmq.AuthLevel.admin

        self.omq.listen(config.HIVEMIND_SOCK, False, allow_connection=allow_local_conn)
        logger.info(f"Listening for local connections on {config.HIVEMIND_SOCK}")

        if config.HIVEMIND_CURVE:
            curve_admins = (
                set(bytes.fromhex(x) for x in re.split("[\s,]+", config.HIVEMIND_CURVE_ADMIN))
                if config.HIVEMIND_CURVE_ADMIN
                else set()
            )

            def allow_curve_conn(addr, pk, sn):
                is_admin = pk in curve_admins
                logger.info(f"Incoming {'admin' if is_admin else 'public'} connection from {addr}")
                return oxenmq.AuthLevel.admin if is_admin else oxenmq.AuthLevel.none

            self.omq.listen(config.HIVEMIND_CURVE, curve=True, allow_connection=allow_curve_conn)

            curve_addr = config.HIVEMIND_CURVE
            if curve_addr.startswith("tcp://"):
                curve_addr = "curve://" + curve_addr[6:]
                curve_addr += "/" + to_base32z(self.omq.pubkey)
            logger.info(f"Listening for incoming connections on {curve_addr}")

        # Notification sent back by oxend when a new block arrives:
        notify = self.omq.add_category("notify", oxenmq.AuthLevel.basic)
        notify.add_command("block", self._on_new_block)
        # Notification sent from swarms when a new message arrives:
        notify.add_command("message", self._on_message_notification)

        push = self.omq.add_category("push", oxenmq.AuthLevel.none)

        # Adds/updates a subscription.  This is called from the HTTP process to pass along an
        # incoming (re)subscription.  The request must be json such as:
        #
        # {
        #     "pubkey": "05123...",
        #     "session_ed25519": "abc123...",
        #     "subkey_tag": "def789...",
        #     "namespaces": [-400,0,1,2,17],
        #     "data": true,
        #     "sig_ts": 1677520760,
        #     "signature": "f8efdd120007...",
        #     "service": "apns",
        #     "service_info": { ... },
        #     "enc_key": "abcdef..." (32 bytes: 64 hex or 43 base64).
        # }
        #
        # The `service_info` argument is passed along to the underlying notification provider and
        # must contain whatever info is required to send notifications to the device: typically some
        # device ID, and possibly other data.  It is specific to each notification provider.
        #
        # The reply is JSON; an error looks like:
        #     { "error": 123, "message": "Something getting wrong!" }
        # where "error" is one of the hive/subscription.py SUBSCRIBE enum values.  On a successful
        # subscription you get back one of:
        #     { "success": true, "added": true, "message": "Subscription successful" }
        #     { "success": true, "updated": true, "message": "Resubscription successful" }
        #
        # Note that the "message" strings are subject to change and should not be relied on
        # programmatically; instead rely on the "error" or "success" values.
        #
        # TODO: make this endpoint directly callable via public oxenmq listener instead of only
        # proxied from HTTP.
        push.add_request_command("subscribe", self._on_subscribe)
        push.add_request_command("unsubscribe", self._on_unsubscribe)

        # Endpoint for local services to talk to us:
        admin = self.omq.add_category("admin", oxenmq.AuthLevel.admin)

        # Registers a notification service.  This gets called with a single argument containing the
        # service name(s) (e.g. "apns", "firebase") that should be pushed to this connection when
        # notifications or subscriptions arrive.  (If a single connection provides multiple services
        # it should invoke this endpoint multiple times).
        #
        # The invoking OMQ connection must accept two commands:
        #
        # `notifier.validate` request command.  This is called on an incoming subscription or
        # unsubscription to validate and parse it.  It is passed a two-part message: the service
        # name (e.g. b"apns") that the client requested, and the JSON registration data as supplied
        # by the client.  The return is one of:
        # - [b'0', b'unique service id', b'supplemental data']  (acceptable registration)
        # - [b'0', b'unique service id']   (acceptable, with no supplemental data)
        # - [b'4', b'Error string']  (non-zero code: code and error message returned to the client)
        # where the unique service id must be a utf8-encoded string that is at least 32 characters
        # long and unique for the device/app in question (if the same service id for the same
        # service already exists, the registration is replaced; otherwise it is a new registration).
        # The supplemental data will be stored and passed along when notifications are provided to
        # the following command.  The remote should *not* store local state associated with the
        # registration: instead everything is meant to be stored by the hivemind caller and then
        # passed back in (via the following endpoint).
        #
        # `notifier.push` is a (non-request) command.  This is called when a user is to be notified
        # of an incoming message.  It is a single-part, bencoded dict containing:
        #
        # - '' -- the service name, e.g. b"apns"
        # - '&' -- the unique service id (as was provided by the validate endpoint).
        # - '!' -- supplemental service data, if the validate request returned any; omitted
        #   otherwise.
        # - '^' -- the xchacha20-poly1305encryption key the user gave when registering for
        #   notifications with which the notification payload should be encrypted.
        # - '#' -- the message hash from storage server.
        # - '@' -- the account ID (Session ID or closed group ID) to which the message was sent (33
        #   bytes).
        # - 'n' -- the swarm namespace to which the message was deposited (-32768 to 32767).
        # - '~' -- the encrypted message data; this field will not be present if the registration
        #   did not request data.
        #
        admin.add_command("register_service", self._on_reg_service)

        # Called periodically to notify us of notifier stats (notifications, failures, etc.)
        admin.add_command("service_stats", self._on_service_stats)

        # Retrieves current statistics
        admin.add_request_command("get_stats", self._get_stats)

        self.sns = {}  # { xpk: ServiceNode, ... }
        self.swarms = {}  # { swarmid: {ServiceNode, ...}, ... }
        self.swarm_ids = []  # sorted list of swarm ids
        self.subscribers = {}  # {SwarmPubkey: [Subscription, ...], ...} -- all subscribers
        self.last_block_hash = ""
        self.last_block_height = -1
        self.notifier_stats = {}
        self.pending_connects = 0
        self.connect_count = 0
        self.last_stats_log = 0.0

        self.startup_time = time.time()

        self.filter = (
            set()
        )  # contains Blake2B(service || svcid || msghash) for sent notification de-duping
        self.filter_decay = (
            set()
        )  # Every 10 minutes, `filter` rotates into this (so that we have 20 minutes of filters)
        self.filter_rotate = time.time() + config.FILTER_LIFETIME

        self.services = {}  # { 'service': oxenmq.ConnectionID, ... }

        with self.lock:

            self._db_cleanup()
            self._load_saved_subscriptions()

            self.notifiers_ready = False
            self.notifiers_ready_cv = Condition(self.lock)

            logger.info("Starting OxenMQ")

            self.omq.start()

            logger.info("Started OxenMQ")

            wait_until = time.time() + config.NOTIFIER_WAIT

            logger.info(f"Connecting to oxend @ {oxend_addr.full_address}")
            self.oxend = self.omq.connect_remote(oxend_addr, auth_level=oxenmq.AuthLevel.basic)
            logger.info("Waiting for oxend connection...")

            self.omq.request_future(self.oxend, "ping.ping").get()
            logger.info("Connected to oxend")

            # Wait for notification servers that start up before or alongside us to connect:
            wait_time = wait_until - time.time()
            if wait_time > 0:
                logger.info(f"Waiting {wait_time} for notifiers to register")
                self.lock.release()
                time.sleep(wait_time)
                self.lock.acquire()

            self.notifiers_ready = True
            self.notifiers_ready_cv.notify_all()

            self._refresh_sns()

            logger.info("Startup complete")

            self.omq.add_timer(self._db_cleanup, timedelta(seconds=30))
            self.omq.add_timer(self._subs_tick, timedelta(seconds=config.SUBS_INTERVAL))

    def check_notifiers_ready(self):
        """
        Make sure we have fully started and allowed time for notification ; if not, block until
        startup is complete.  (This might end up blocking all the worker threads, but that's
        desired: we only delay for a second or two at startup).
        """
        # Variable access is atomic in Python so we don't have to acquire the lock before an initial
        # check:
        if not self.notifiers_ready:
            with self.lock:
                self.notifiers_ready_cv.wait_for(lambda: self.notifiers_ready)

    def add_subscription(
        self,
        pubkey: Union[SwarmPubkey, bytes],
        *,
        session_ed25519: Optional[bytes] = None,
        service: str,
        service_id: str,
        service_data: Optional[bytes] = None,
        **kwargs,
    ):
        """Add or updates a subscription for monitoring.  If the given pubkey is already monitored
        by the same given subkey (if applicable) and same namespace/data values then this replaces
        the existing subscription, otherwise it adds a new subscription.

        Will throw if the given data or signatures are incorrect.

        Returns True if the subscription was brand new, False if the subscription updated/renewed an
        existing subscription.

        Parameters:

        - pubkey -- the account to monitor; this can either be a pre-construted SwarmPubkey or the
          33 byte pubkey (including the network prefix).
        - session_ed25519 -- if passing `pubkey` as bytes and using an 05-prefixed Session ID pubkey
          then this must be provided as well and must be set to the 32-byte Ed25519 pubkey
          associated with the Session ID.
        - service -- the subscription service name, e.g. 'apns', 'firebase'.  When messages are
          received the notification will be forwarded to the given service, if active.
        - service_id -- an identifier string that identifies the device/application/etc.  This must
          be unique for a given service and pubkey (if all three match, an existing subscription
          will be replaced).
        - service_data -- optional service data; this will be passed as-is to the service handler
          and contains any extra data (beyond just the service_id) needed for the service handler to
          send the notification to the device.  May be none if no extra data is needed.
        - **kwargs -- any other keyword arguments are passed to the Subscription constructor; see it
          for required arguments.
        """

        if isinstance(pubkey, SwarmPubkey):
            pk = pubkey
        else:
            pk = SwarmPubkey(pubkey, session_ed25519)

        sub = Subscription(pk, **kwargs)

        new_sub = False

        with db_pool.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    id,
                    subkey_tag,
                    signature_ts,
                    ARRAY(SELECT namespace FROM sub_namespaces WHERE subscription = id ORDER BY namespace)
                FROM subscriptions
                WHERE
                    account = %s AND
                    service = %s AND
                    svcid = %s
                    """,
                (sub.pubkey.id, service, service_id),
            )
            row = cur.fetchone()
            insert_ns = False
            if row is None:
                new_sub = True
                cur.execute(
                    """
                    INSERT INTO subscriptions
                        (account, session_ed25519, subkey_tag, signature, signature_ts, want_data, enc_key, service, svcid, svcdata)
                    VALUES (%s,   %s,              %s,         %s,        %s,           %s,        %s,      %s,      %s,    %s)
                    RETURNING id
                    """,
                    (
                        sub.pubkey.id,
                        sub.pubkey.ed25519_pubkey if sub.pubkey.id[0] == 0x05 else None,
                        sub.subkey_tag,
                        sub.sig,
                        sub.sig_ts,
                        sub.want_data,
                        sub.enc_key,
                        service,
                        service_id,
                        service_data,
                    ),
                )

                id = cur.fetchone()[0]
                insert_ns = True

            else:
                id, subkey_tag, sig_ts, ns = row

                insert_ns = ns != sub.namespaces
                if sub.sig_ts > sig_ts or subkey_tag != sub.subkey_tag or insert_ns:
                    cur.execute(
                        """
                        UPDATE subscriptions
                        SET subkey_tag = %s, signature = %s, signature_ts = %s, svcdata = %s
                        WHERE id = %s
                        """,
                        (sub.subkey_tag, sub.sig, sub.sig_ts, service_data, id),
                    )
                if insert_ns:
                    cur.execute("DELETE FROM sub_namespaces WHERE subscription = %s", (id,))

            if insert_ns:
                for n in sub.namespaces:
                    cur.execute(
                        "INSERT INTO sub_namespaces (subscription, namespace) VALUES (%s, %s)",
                        (id, n),
                    )

        with self.lock:
            sub.pubkey.update_swarm(self.swarm_ids)
            new_sub = self.subscribers.add(sub)

            # If this is actually adding a new subscription (and not just renewing an existing one)
            # then we need to force subscription (or resubscription) on all of the account's swarm
            # members to get the subscription active ASAP.  (Otherwise don't do anything because we
            # already have an equivalent subscription in place).
            if new_sub:
                for sn in self.swarms.setdefault(pk.swarm, set()):
                    sn.add_account(pk, force_now=True)

        return new_sub

    def remove_subscription(
        self,
        pubkey: Union[SwarmPubkey, bytes],
        *,
        session_ed25519: Optional[bytes] = None,
        subkey_tag: Optional[bytes] = None,
        service: str,
        service_id: str,
        sig: bytes,
        sig_ts: int,
        **kwargs,
    ):
        """
        Removes a subscription for monitoring.  Returns True if the given pubkey was found and
        removed; False if not found.

        Will throw if the given data or signatures are incorrect.

        Parameters:

        - pubkey -- the account; this can either be a pre-construted SwarmPubkey or the 33 byte
          pubkey (including the network prefix).
        - session_ed25519 -- if passing `pubkey` as bytes and using an 05-prefixed Session ID pubkey
          then this must be provided as well and must be set to the 32-byte Ed25519 pubkey
          associated with the Session ID.
        - subkey_tag -- if using subkey authentication then this is the 32-byte subkey tag.
        - service -- the subscription service name, e.g. 'apns', 'firebase'.
        - service_id -- an identifier string that identifies the device/application/etc.  This is
          unique for a given service and pubkey and is generated/extracted by the notification
          service.
        - sig_ts -- the integer unix timestamp when the signature was generated; must be within ±24h
        - signature -- the Ed25519 signature of: UNSUBSCRIBE || PUBKEY_HEX || sig_ts
        """

        if isinstance(pubkey, SwarmPubkey):
            pk = pubkey
        else:
            pk = SwarmPubkey(pubkey, session_ed25519)

        now = int(time.time())
        if not sig_ts or now + UNSUBSCRIBE_GRACE < sig_ts < now - UNSUBSCRIBE_GRACE:
            raise ValueError("Invalid signature: sig_ts is too far from current time")

        # "UNSUBSCRIBE" || HEX(ACCOUNT) || SIG_TS
        sig_msg = f"UNSUBSCRIBE{pk.id.hex()}{sig_ts:d}".encode()

        try:
            verify_storage_signature(
                sig_msg, signature=sig, ed25519_pubkey=pk.ed25519_pubkey, subkey_tag=subkey_tag
            )
        except BadSignatureError:
            raise ValueError("Invalid signature: signature verification failed")

        removed = False

        with db_pool.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM subscriptions
                WHERE
                    account = %s AND
                    service = %s AND
                    svcid = %s
                    """,
                (pk.id, service, service_id),
            )
            if cur.rowcount > 0:
                removed = True

        # We don't need to bother removing it from self.whatever data structures: as long as the row
        # is removed (above) we won't be sending notifications to the device anymore.
        return removed

    def _db_cleanup(self):
        with db_pool.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM subscriptions WHERE signature_ts <= %s",
                (int(time.time()) - SIGNATURE_EXPIRY,),
            )

    def _load_saved_subscriptions(self):
        # with self.lock: ## already held
        with db_pool.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT account, session_ed25519, subkey_tag, signature, signature_ts, want_data, enc_key,
                ARRAY(SELECT namespace FROM sub_namespaces WHERE subscription = id ORDER BY namespace)
                FROM subscriptions
                """
            )
            count = 0
            for row in cur:
                pk = SwarmPubkey(row[0], row[1])
                self.subscribers.setdefault(pk, []).append(
                    Subscription(
                        pk,
                        subkey_tag=row[2],
                        sig=row[3],
                        sig_ts=row[4],
                        want_data=row[5],
                        enc_key=row[6],
                        namespaces=row[7],
                        _skip_validation=True,
                    )
                )
                count += 1

            logger.info(f"Loaded {count} subscriptions from database")

    @warn_on_except
    def _on_notifier_validation(
        self,
        success: bool,
        sub_or_unsub: bool,  # True = subscribe, False = unsubscribe
        reply: oxenmq.Message.DeferredSend,
        sub_args: dict,
        data: list[memoryview],
    ):
        response = {}  # Will have 'error'/'success', 'message', and maybe other things added
        code, message = SUBSCRIBE.ERROR, "Unknown error"

        service = sub_args["service"]

        try:
            if not success:
                logger.critical(f"Communication with {service} failed: {data}")
                if data and data[0] == b"TIMEOUT":
                    raise SubscribeError(
                        SUBSCRIBE.SERVICE_TIMEOUT, f"{service} notification service timed out"
                    )
                raise SubscribeError(
                    SUBSCRIBE.ERROR, f"failed to communicate with {service} notification service"
                )

            if not 2 <= len(data) <= 3:
                raise ValueError(f"invalid {len(data)}-part response: {data}")

            code = int(data[0])
            d1 = str(data[1], "utf8")
            if code == 0:
                service_id = d1
                if len(service_id) < 32:
                    raise ValueError(f"service id too short ({len(service_id)} < 32)")

                sub_args["service_id"] = service_id
                if sub_or_unsub:  # New/renewed subscription:
                    sub_args["service_data"] = data[2] if len(data) >= 3 else None
                    newsub = self.add_subscription(**sub_args)
                    response["added" if newsub else "updated"] = True
                    code, message = (
                        SUBSCRIBE.OK.value,
                        f"{'S' if newsub else 'Res'}ubscription successful",
                    )
                else:  # Unsubscribe
                    removed = self.remove_subscription(**sub_args)
                    response["removed"] = bool(removed)
                    code, message = (
                        SUBSCRIBE.OK.value,
                        "Device unsubscribed from push notifications"
                        if removed
                        else "Device was not subscribed to push notifications",
                    )

            else:
                message = d1

        except SubscribeError as e:
            code, message = e.code.value, str(e)

        except ValueError as e:
            code, message = SUBSCRIBE.BAD_INPUT.value, f"Invalid subscription parameters: {e}"

        except Exception as e:
            logger.critical(
                f"An exception occurred while processing {service} validation response: {e}"
            )
            code, message = (
                SUBSCRIBE.ERROR.value,
                "Internal error: notification service returned invalid response",
            )

        if code == SUBSCRIBE.OK.value:
            response["success"] = True
        else:
            response["error"] = code
        if message:
            response["message"] = message
        reply(json.dumps(response))

    def _on_subscribe(self, msg: oxenmq.Message):
        code, message = None, None  # If still None at the end, we send a reply
        try:
            args = json.loads(msg.data()[0])

            pubkey = extract_hex_or_b64(args, "pubkey", 33)
            session_ed = None
            if pubkey.startswith(b"\x05"):
                session_ed = extract_hex_or_b64(args, "session_ed25519", 32)
            subkey_tag = extract_hex_or_b64(args, "subkey_tag", 32, required=False)
            sig_ts = args["sig_ts"]
            if not isinstance(sig_ts, int):
                raise ValueError("Invalid or missing sig_ts parameter")
            sig = extract_hex_or_b64(args, "signature", 64)
            enc_key = extract_hex_or_b64(args, "enc_key", 32)

            service = args["service"]
            service_info = args["service_info"]

            with self.lock:
                self.check_notifiers_ready()
                conn = self.services.get(service)

            if not conn:
                raise SubscribeError(
                    SUBSCRIBE.SERVICE_NOT_AVAILABLE,
                    f"{service} notification service not currently available",
                )

            # We handle everything else (including the response) in `_on_notifier_validation`
            # when/if the notifier service comes back to us with the unique identifier:
            sub_args = dict(
                service=service,
                pubkey=pubkey,
                session_ed25519=session_ed,
                subkey_tag=subkey_tag,
                namespaces=args["namespaces"],
                want_data=bool(args.get("data")),
                sig_ts=sig_ts,
                sig=sig,
                enc_key=enc_key,
            )
            replier = msg.later()
            self.omq.request(
                conn,
                "notifier.validate",
                service,
                json.dumps(service_info),
                on_reply=partial(self._on_notifier_validation, True, True, replier, sub_args),
                on_reply_failure=partial(
                    self._on_notifier_validation, False, True, replier, sub_args
                ),
            )
        except json.decoder.JSONDecodeError:
            logger.debug("Subscription failed: bad json")
            code, message = SUBSCRIBE.BAD_INPUT, "Invalid JSON"
        except KeyError as e:
            logger.debug(f"Sub failed: missing param {e}")
            code, message = SUBSCRIBE.BAD_INPUT, f"Missing required parameter {e}"
        except SubscribeError as e:
            logger.debug(f"Subscribe error: {e} ({e.code})")
            code, message = e.code, str(e)
        except Exception as e:
            logger.debug(f"Exception handling input: {e}")
            code, message = SUBSCRIBE.ERROR, str(e)

        if code is not None:
            logger.debug(f"Replying with error code {code}: {message}")
            msg.reply(json.dumps({"error": code.value, "message": message}))
        # Otherwise the reply is getting deferred and handled later

    def _on_unsubscribe(self, msg: oxenmq.Message):
        code, message = None, None  # If still None at the end, we send a reply
        try:
            args = json.loads(msg.data()[0])

            pubkey = extract_hex_or_b64(args, "pubkey", 33)
            session_ed = None
            if pubkey.startswith(b"\x05"):
                session_ed = extract_hex_or_b64(args, "session_ed25519", 32)
            subkey_tag = extract_hex_or_b64(args, "subkey_tag", 32, required=False)
            sig_ts = args["sig_ts"]
            if not isinstance(sig_ts, int):
                raise ValueError("Invalid or missing sig_ts parameter")
            sig = extract_hex_or_b64(args, "signature", 64)

            service = args["service"]
            service_info = args["service_info"]

            with self.lock:
                self.check_notifiers_ready()
                conn = self.services.get(service)

            if not conn:
                raise SubscribeError(
                    SUBSCRIBE.SERVICE_NOT_AVAILABLE,
                    f"{service} notification service not currently available",
                )

            # We handle everything else (including the response) in `_on_notifier_validation`
            # when/if the notifier service comes back to us with the unique identifier:
            sub_args = dict(
                service=service,
                pubkey=pubkey,
                session_ed25519=session_ed,
                subkey_tag=subkey_tag,
                sig_ts=sig_ts,
                sig=sig,
            )
            replier = msg.later()
            self.omq.request(
                conn,
                "notifier.validate",
                service,
                json.dumps(service_info),
                on_reply=partial(self._on_notifier_validation, True, False, replier, sub_args),
                on_reply_failure=partial(
                    self._on_notifier_validation, False, False, replier, sub_args
                ),
            )
        except json.decoder.JSONDecodeError:
            logger.debug("Subscription failed: bad json")
            code, message = SUBSCRIBE.BAD_INPUT, "Invalid JSON"
        except KeyError as e:
            logger.debug(f"Sub failed: missing param {e}")
            code, message = SUBSCRIBE.BAD_INPUT, f"Missing required parameter {e}"
        except SubscribeError as e:
            logger.debug(f"Subscribe error: {e} ({e.code})")
            code, message = e.code, str(e)
        except Exception as e:
            logger.debug(f"Exception handling input: {e}")
            code, message = SUBSCRIBE.ERROR, str(e)

        if code is not None:
            logger.debug(f"Replying with error code {code}: {message}")
            msg.reply(json.dumps({"error": code.value, "message": message}))
        # Otherwise the reply is getting deferred and handled later

    def _refresh_sns(self):
        self.omq.request(
            self.oxend,
            "rpc.get_service_nodes",
            _get_sns_params,
            on_reply=self._on_sns_response,
            on_reply_failure=lambda err: logger.warning(f"get_service_nodes request failed: {err}"),
        )

    def _on_new_block(self, _: oxenmq.Message):
        with self.lock:
            self._refresh_sns()

    def _on_sns_response_impl(self, data: list[memoryview]):
        if len(data) != 2:
            logger.warning(f"rpc.get_service_nodes returned unexpected {len(data)}-length response")
            return
        code, res = data[0].tobytes(), data[1].tobytes()
        if code != b"200":
            logger.warning(f"rpc.get_service_nodes returned unexpected response {code}: {res}")
            return

        try:
            res = json.loads(res)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse rpc.get_service_nodes response: {e}")
            return

        sns = res.get("service_node_states")
        if not sns or not isinstance(sns, list):
            logger.warning(
                "Unexpected rpc.get_service_nodes response: service_node_states looks wrong"
            )
            return

        swarms_changed = False
        last_hash = res.get("block_hash", "")
        if last_hash != self.last_block_hash:
            logger.debug(f"new block hash {last_hash}")
            # The block changed, so we need to check for swarm changes as well
            new_swarms = sorted(
                set(x["swarm_id"] for x in sns if x["swarm_id"] != INVALID_SWARM_ID)
            )
            if new_swarms != self.swarm_ids:
                swarms_changed = True
                self.swarm_ids = new_swarms
                for s in self.swarm_ids:
                    self.swarms.setdefault(s, set())

            self.last_block_hash = last_hash
            self.last_block_height = res.get("height", -1)

        missing_count = len(sns)
        sns = [
            (x["pubkey_x25519"], x["public_ip"], x["storage_lmq_port"], x["swarm_id"])
            for x in sns
            if (
                # If we don't have any of this info then this is probably a new SN that hasn't sent
                # a proof yet; treat it as if we didn't get it at all.
                len(x["pubkey_x25519"]) == 64
                and x["public_ip"] != ""
                and x["public_ip"] != "0.0.0.0"
                and x["storage_lmq_port"] != 0
                and x["swarm_id"] != INVALID_SWARM_ID
            )
        ]
        missing_count -= len(sns)

        logger.debug(f"{len(sns)} active SNs ({missing_count} missing details)")

        nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
        if nofile[0] < len(sns):
            logger.critical(
                f"file description ulimit is {nofile[0]}, but there are {len(sns)} service nodes.  Connections are likely going to fail!"
            )

        # Anything in self.sns but not in sns is no longer on the network (decommed, dereged,
        # expired), or possibly we lost info for it (from the above).  We're going to disconnect
        # from these (if any are connected).
        old_sns = set(self.sns.keys()) - set(s[0] for s in sns)

        new_or_changed_sns = set()

        for xpk, ip, port, swarm in sns:
            addr = oxenmq.Address(f"curve://{ip}:{port}/{xpk}")

            if xpk in self.sns:
                # We already know about this service node from the last update, but it might have
                # changed address or swarm, in which case we want to disconnect and then store it as
                # "new" so that we reconnect to it (if required) later.  (We don't technically have
                # to reconnect if swarm changes, but it simplifies things a bit to do it anyway).

                snode = self.sns[xpk]
                if swarm != snode.swarm:
                    self.swarms[snode.swarm].remove(snode)
                    snode.reset_swarm(swarm)
                    self.swarms[swarm].add(snode)
                    new_or_changed_sns.add(snode)

                snode.connect(addr)  # Reconnects if necessary

            else:
                snode = SNode(logger, self, self.omq, addr, swarm)
                self.sns[xpk] = snode
                self.swarms[swarm].add(snode)
                new_or_changed_sns.add(snode)

        logger.debug(f"{len(new_or_changed_sns)} new/updated SNs; dropping {len(old_sns)} old SNs")

        # Remove/disconnect from any SNs that aren't active SNs anymore
        for xpk in old_sns:
            logger.debug(f"Disconnecting {xpk}")
            snode = self.sns[xpk]
            self.swarms[snode.swarm].remove(snode)
            snode.disconnect()
            del self.sns[xpk]

        # If we had a change to the network's swarms then we need to trigger a full recheck of swarm
        # membership, ejecting any pubkeys that moved while adding all pubkeys again to be sure they
        # are in each (possibly new) slot.
        if swarms_changed:
            sw_changes = 0
            for s in self.subscribers.keys():
                sw_changes += s.update_swarm(self.swarm_ids)

            logger.debug(f"{sw_changes} accounts changed swarms")

            for snode in self.sns.values():
                snode.recheck_swarm_members()

            for subscriber in self.subscribers.keys():
                for snode in self.swarms[subscriber.swarm]:
                    snode.add_account(subscriber)

            self._check_subs()

        elif new_or_changed_sns:
            # Otherwise swarms stayed the same (which means no accounts changed swarms), but snodes
            # might have moved in/out of existing swarms, so re-add any subscribers to swarm
            # changers to ensure they have all the accounts that belong to them.
            swarm_subs = {snode.swarm: [] for snode in new_or_changed_sns}
            for subscriber in self.subscribers.keys():
                sw_subs = swarm_subs.get(subscriber.swarm)
                if sw_subs:
                    sw_subs.append(subscriber)

            for snode in new_or_changed_sns:
                for subscriber in swarm_subs[snode.swarm]:
                    snode.add_account(subscriber)

            self._check_subs()

    def _on_sns_response(self, data: list[memoryview]):
        try:
            with self.lock:
                self._on_sns_response_impl(data)
        except Exception as e:
            logger.warning(f"An exception occured while processing the SN update: {e}")

    def _allow_connect(self):
        """Called when initiating a connection: if this returns True then the connection can proceed; if it returns False then the connection should not.

        If the connection attempt proceeds, the caller must call finish_connect() in the connection
        success or failure function to allow other connections to be attempted.
        """
        if self.pending_connects > config.MAX_PENDING_CONNECTS:
            return False
        self.connect_count += 1
        self.pending_connects += 1
        logger.debug(f"establishing connection pc={self.pending_connects}, cc={self.connect_count}")
        return True

    def _finished_connect(self):
        """Counterpart to _allow_connect() that allows other connections to proceed (if rate limited)."""
        try_more = self.pending_connects == config.MAX_PENDING_CONNECTS
        self.pending_connects -= 1
        if try_more:
            self._check_subs()

    def _check_subs(self):
        """Triggers sending of any pending (re-)subscriptions to snodes"""
        for xpk, snode in self.sns.items():
            try:
                snode.check_subs()
            except Exception as e:
                logger.warning(
                    f"Failed to check subs on {xpk}:\n" + "\n".join(traceback.format_exception(e))
                )

    def _subs_tick(self):
        # Ignore the confirm response from this; we can't really do anything with it, we just want
        # to make sure we stay subscribed.
        self.omq.request(self.oxend, "sub.block")

        with self.lock:
            self._check_subs()

            now = time.time()
            if self.last_stats_log + 3595 < now:
                self.last_stats_log = now
                n_accounts = len(self.subscribers)
                n_subs = sum(len(subs) for subs in self.subscribers.values())
                logger.info(
                    f"PN subscription stats: handling subscriptions for {n_accounts} accounts "
                    f"({n_subs} subscriptions)"
                )

    def _on_reg_service(self, message: oxenmq.Message):
        try:
            d = message.data()
            if len(d) != 1:
                raise RuntimeError(f"{len(d)}-part data, expected 1")
            if len(d[0]) > 32:
                raise RuntimeError(f"service name too long ({len(d[0])})")
            name = d[0].decode()

        except Exception as e:
            logger.critical(f"Invalid push service registration: {e}")
            return

        with self.lock:
            self.services[name] = message.conn

        logger.info(f"'{name}' notification service registered")

    def _on_message_notification(self, message: oxenmq.Message):
        d = message.dataview()
        if len(d) != 1:
            logger.warning(f"Unexpected notification: {len(d)}-part data, expected 1")
            return
        data = bt_deserialize(d[0])

        if not isinstance(data, dict):
            logger.warning(f"Unexpected notification: not a dict")
            return

        msghash = data[b"h"]

        logger.debug(
            f"Got a notification for {data[b'@'].hex()}, msg hash {msghash}, "
            f"namespace {data[b'n']}, timestamp {data[b't']}, exp {data[b'z']}, "
            f"data {len(data[b'~']) if b'~' in data else '(N/A)'}B"
        )

        notifies = []

        with db_pool.connection() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT want_data, enc_key, service, svcid, svcdata FROM subscriptions
                WHERE account = %s AND EXISTS(
                    SELECT 1 FROM sub_namespaces WHERE subscription = id AND namespace = %s)
                """,
                (data[b"@"], data[b"n"]),
            )
            for row in cur:
                want_data, enc_key, service, s_id, s_data = row
                s_id = s_id.encode()
                h = blake2b(digest_size=32)
                h.update(service.encode())
                h.update(s_id)
                h.update(data[b"h"])
                filter_val = h.digest()

                notifies.append((want_data, enc_key, service, s_id, s_data, filter_val))

        if not notifies:
            logger.debug(f"No active notifications match, ignoring notification")
            return

        with self.lock:
            now = time.time()
            if now >= self.filter_rotate:
                self.filter, self.filter_decay = set(), self.filter
                self.filter_rotate = now + config.FILTER_LIFETIME

            for want_data, enc_key, service, svcid, svcdata, filter_val in notifies:
                conn = self.services.get(service)
                if not conn:
                    logger.warning(f"Notification depends on unregistered service {service}")
                    continue

                if filter_val in self.filter or filter_val in self.filter_decay:
                    logger.debug("Ignoring duplicate notification")
                    continue

                self.filter.add(filter_val)

                push_data = {
                    "": service,
                    "&": svcid,
                    "^": enc_key,
                    "#": msghash,
                    "@": data[b"@"],
                    "n": data[b"n"],
                }
                maybe_data = data.get(b"~")
                if maybe_data is not None:
                    push_data["~"] = maybe_data
                if svcdata is not None:
                    push_data["!"] = svcdata

                logger.debug(f"Sending push to {service}")
                self.omq.send(conn, "notifier.push", bt_serialize(push_data))

    @warn_on_except
    def _on_service_stats(self, message: oxenmq.Message):
        """
        Called from a notifier service periodically to report statistics.

        This should be called with a two-part message: the first part is the service name (e.g.
        'apns'); the second part is a bt-encoded dict with content such as:

        {
            'notifies': 12,
            'failures': 0
        }

        The local stats for this notifier will be *incremented* with the given values.

        Other keys can be included; if present they will be incremented (if integers) or replaced
        (otherwise).  The returned notifier stats are included in the admin.get_stats OMQ RPC call
        for this service.
        """

        logger.warning("on_service_stats!")

        d = message.data()
        if len(d) != 2:
            logger.warning("Invalid admin.service_stats call: expected 2-part message")
            return

        service = d[0].decode()

        try:
            stats = bt_deserialize(d[1])
        except Exception as e:
            logger.warning(
                f"Invalid admin.service_stats call: could not parse bt-encoded message ({e})"
            )
            return

        svc_stats = self.notifier_stats.setdefault(service, {})

        with self.lock:
            for k, v in stats.items():
                k = k.decode()
                if isinstance(v, int):
                    if k not in svc_stats or not isinstance(svc_stats[k], int):
                        svc_stats[k] = 0
                    svc_stats[k] += v
                else:
                    svc_stats[k] = v

    @warn_on_except
    def _get_stats(self, message: oxenmq.Message):
        with self.lock:
            stats = {
                "block_hash": self.last_block_hash,
                "block_height": self.last_block_height,
                "swarms": len(self.swarms),
                "snodes": len(self.sns),
                "connections": sum(s.connected for s in self.sns.values()),
                "accounts": len(self.subscribers),
                "subscriptions": sum(len(subs) for subs in self.subscribers.values()),
                "notifications": deepcopy(self.notifier_stats),
                "uptime": time.time() - self.startup_time,
            }

        message.reply(json.dumps(stats))


def run():
    """Runs a HiveMind instance indefinitely (intended for use as a uwsgi mule)"""

    try:
        logger.info("Starting hivemind")
        hivemind = HiveMind()

        logger.info("Hivemind started")

        while True:
            time.sleep(1)
    except Exception as e:
        logger.critical(f"HiveMind failed: {e}")
