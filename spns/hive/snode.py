from __future__ import annotations
from .swarmpubkey import SwarmPubkey

from collections import deque
from oxenc import bt_serialize
import oxenmq
import random
import time
from typing import Optional


# Maximum size of simultaneous subscriptions in a single subscription request; if we overflow then
# any stragglers wait until the next request, delaying them by a few seconds.  (This is not a rock
# hard limit: we estimate slightly and stop as soon as we exceed it, which means we can go over it a
# bit after appending the last record).
SUBS_REQUEST_LIMIT = 5_000_000

# How long (in seconds) after a successful subscription before we re-subscribe; each subscription
# gets a uniform random value between these two values (to spread out the renewal requests a bit).
RESUBSCRIBE_MIN = 45 * 60
RESUBSCRIBE_MAX = 55 * 60

# How long we wait (in seconds) after a connection failure to a snode storage server before
# re-trying the connection; we use the first value after the first failure, the second one after the
# second failure, and so on (if we run off the end we use the last value).
CONNECT_COOLDOWN = [10, 30, 60, 120]


class SNode:
    """Class managing a connection to a single service node"""

    def __init__(
        self,
        logger,
        hivemind: HiveMind,
        omq: oxenmq.OxenMQ,
        addr: oxenmq.Address,
        swarm: Optional[int],
    ):
        self.logger = logger
        self.hivemind = hivemind
        self.omq = omq
        self.addr = addr
        self.swarm = swarm
        self.conn = None
        self.connected = False
        self.subs = set()  # SwarmPubkey
        self.next = deque()  # [(SwarmPubkey, next), ...] sorted by next re-subscription time
        self.cooldown_until = None
        self.cooldown_fails = 0

        self.connect()

    def __del__(self):
        self.disconnect()

    def connect(self, addr: Optional[oxenmq.Address] = None):
        """Initiates a connection, if not already connected.  If addr is given and doesn't match the
        current address then we drop the current connection first.

        Does nothing if already connected, or already establishing a connection.

        After this call `self.conn` will be set to the ConnectionID of the established or
        establishing connection."""
        if addr is not None and addr != self.addr:
            self.logger.debug(f"disconnecting; addr changing from {self.addr} to {addr}")
            self.disconnect()
            self.addr = addr

        if self.conn is None:
            if self.hivemind._allow_connect():
                self.conn = self.omq.connect_remote(
                    self.addr,
                    on_success=self.on_connected,
                    on_failure=self.on_connect_fail,
                    auth_level=oxenmq.AuthLevel.basic,
                )
                self.logger.debug(f"Establishing connection to {self.addr.full_address}")

    def disconnect(self):
        self.logger.debug(f"disconnecting from {self.addr.full_address}")
        self.connected = False
        if self.conn is not None:
            self.omq.disconnect(self.conn)
            self.conn = None

    def on_connected(self, conn: oxenmq.ConnectionID):
        with self.hivemind.lock:
            self.logger.debug(f"Connected established to {self.addr.full_address}")
            self.cooldown_fails = 0
            self.cooldown_until = None
            self.hivemind._finished_connect()
            if self.conn is None:
                # Our conn got replaced from under us, which probably means we are disconnecting, so do
                # nothing.
                pass

            self.connected = True
            # We either just connected or reconnected, so reset any re-subscription times (so that after
            # a reconnection we force a re-subscription for everyone):
            for n in self.next:
                n[1] = 0.0
            self.check_subs(initial_subs=True)

    def on_connect_fail(self, conn: oxenmq.ConnectionID, reason: str):
        with self.hivemind.lock:
            self.hivemind._finished_connect()
            cooldown = CONNECT_COOLDOWN[
                -1 if self.cooldown_fails >= len(CONNECT_COOLDOWN) else self.cooldown_fails
            ]
            self.cooldown_fails += 1
            self.cooldown_until = time.time() + cooldown
            self.logger.warning(
                f"Connection to {self.addr.full_address} failed: {reason} ({self.cooldown_fails} consecutive failure(s); retrying in {cooldown}s)"
            )
            self.disconnect()

    def add_account(self, account: SwarmPubkey, force_now: bool = False):
        """Adds a new account to be signed up for subscriptions, if it is not already subscribed.
        The new account's subscription will be submitted to the SS the next time check_subs() is
        called (either automatically or manually).

        If `force_now` is True then the account is scheduled for subscription at the next update
        even if already exists."""
        if account not in self.subs:
            self.subs.add(account)
            self.next.appendleft((account, 0.0))
        elif force_now:
            for n in self.next:
                if n[0] == account:
                    n[0] = None  # lazy deletion; we'll skip this when draining the queue
                    break
            self.next.appendleft((account, 0.0))

    def reset_swarm(self, swarm: int):
        """Called when swarm changes; all current subscriptions are dropped."""
        self.next.clear()
        self.subs.clear()
        self.swarm = swarm

    def recheck_swarm_members(self):
        """Called when the network swarm list has changed (but this swarm hasn't), to eject any
        swarm subscriptions that don't belong here anymore.  The Accounts in `.subs` should have had
        .update_swarm called already to check/reset the new swarm_id.  Any existing subscribers that
        are no longer in this swarm will be removed.  (Even without a swarm change of this node,
        this can happen if another new swarm is created next to us).

        This isn't responsible for adding *new* swarm members: this is just called as a first step
        for removing any that shouldn't be here anymore."""
        for n in self.next:
            if n[0].swarm != self.swarm:
                self.subs.remove(n[0])
                n[0] = None  # lazy deletion

    def check_subs(self, *, initial_subs=False):
        """
        Check our subscriptions to resubscribe to any that need it.  If initial is True then this is
        the initial request and we fire off a batch of subscriptions and then another batch upon
        reply, etc. until there are no more subs to send; otherwise we fire off just up to
        SUBS_LIMIT re-subscriptions.
        """
        if not self.connected:
            if self.conn is not None:
                return  # We're already trying to connect

            # If we failed recently we'll be in cooldown mode for a while, so might not connect
            # right away yet.
            if self.cooldown_until is not None:
                if self.cooldown_until > time.time():
                    return
                self.cooldown_until = None

            # We'll get called automatically as soon as the connection gets established, so just
            # make sure we are already connecting and don't do anything else for now.
            return self.connect()

        req = []
        now = time.time()
        req_size = 0
        while req_size < SUBS_REQUEST_LIMIT and self.next and self.next[0][1] <= now:
            acct = self.next.popleft()[0]
            if acct is None:  # lazy deletion; ignore this entry
                continue

            subs = self.hivemind.subscribers.get(acct, now)
            if subs is None:
                continue
            continue

            for sub in subs:
                d = {"n": sub.namespaces, "t": sub.sig_ts, "s": sub.sig}

                account = acct.id
                if account[0] == 0x05:
                    d["P"] = acct.ed25519_pubkey
                else:
                    d["p"] = account

                # bencoded serialized lengths of these data.  Each key is 3 (e.g. "1:n"), list
                # and integer markers add another 2 (l...e or i...e), and binary data (for the sizes
                # we use here) adds 3 (32:, 33: or 64:).
                #
                # The 4 here on the number of namespaces is definitely handwavey: if you were to
                # subscribe to *every* single 65536 namespaces then the encoded namespace list
                # (i-32768ei-32767e...) would be 469304 bytes long, which gives an average of 7.16
                # characters per namespace.  But typically Session is subscribing to something more
                # like 0, 1, 2, 3, ..., up to maybe 6 or 7, all of which are 3 (i0e, etc.).  So 4
                # seems reasonable, and is probably likely to overestimate in most cases.
                req_size += 5 + 4 * len(sub.namespaces) + 15 + 70 + 39

                if sub.subkey_tag:
                    d["S"] = sub.subkey_tag
                    req_size += 38  # 1:S32:... = 6 + the ... (32)

                if sub.want_data:
                    d["d"] = 1
                    req_size += 6  # 1:di1e

                req.append(d)

            self.next.append((acct, now + random.uniform(RESUBSCRIBE_MIN, RESUBSCRIBE_MAX)))

        if req:
            on_reply = None
            if initial and len(req) >= SUBS_REQUEST_LIMIT:
                # We're doing the initial subscriptions, so continue as soon as we get the reply so
                # that we subscribe quickly, but never have more than one massive subscription
                # request active at a time
                on_reply = lambda _: self.connect(initial_subs=True)

            # Otherwise we ignore the reply: even if there are some failures, we have redundancy
            # because we are connected to all swarm members, so even if we lose our subscription for
            # a while from this SN, it shouldn't break notifications.

            self.omq.request(self.conn, "monitor.messages", bt_serialize(req), on_reply=on_reply)
            self.logger.debug(f"(Re-)subscribing to {len(req)} accounts from {self.addr}")
