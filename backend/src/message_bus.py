from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from logging import Logger
from typing import TYPE_CHECKING

from models import MessagePayload

# these imports exist only for type annotations. importing them for real would create
# an import cycle (cluster -> nodes -> message_bus -> cluster). with the __future__
# import above, annotations are never evaluated at runtime, so the guard is safe.
if TYPE_CHECKING:
    from cluster import Cluster
    from nodes import RaftNode


log: Logger = logging.getLogger(__name__)


@dataclass
class Message:
    id: int
    src: int
    dst: int
    payload: MessagePayload
    sent_at: float  # event-loop time, seconds (monotonic)
    deliver_at: float


class MessageBus:
    def __init__(
        self,
        cluster: Cluster,
        latency_range: tuple[float, float] = (0.05, 0.15),
        drop_rate: float = 0.0,
        partition: list[set[int]] | None = None,
    ):
        self.messages: dict[int, Message] = {}
        self.cluster = cluster
        self.next_msg_id = 0
        # each message gets a random latency in this range (seconds)
        self.latency_range = latency_range
        # a chance that the message vanishes (simulating some issue)
        self.drop_rate = drop_rate
        self.partition = partition

    # nodes call this and only this. fire-and-forget: no return value, no error —
    # a sender must never be able to learn its message's fate from the bus.
    def send(self, src: int, dst: int, payload: MessagePayload) -> None:
        if self._is_partitioned(src, dst):
            log.debug("bus: n%d -> n%d blocked by partition", src, dst)
            return
        if random.random() < self.drop_rate:
            log.debug("bus: n%d -> n%d dropped due to random.random() < drop rate", src, dst)
            return

        # always use get_running_loop() instead of datetime (which is wall clock)
        # get_running_loop() uses the same clock that asyncio's event loop uses.
        # and since every app has 1 event loop, the timing can never disagree.
        now = asyncio.get_running_loop().time()
        latency = random.uniform(*self.latency_range)
        message = Message(
            id=self.next_msg_id,
            src=src,
            dst=dst,
            payload=payload,
            sent_at=now,
            deliver_at=now + latency,
        )
        self.next_msg_id += 1
        self.messages[message.id] = message

        log.debug(
            "bus: msg %d n%d -> n%d in flight (%.0fms)",
            message.id,
            src,
            dst,
            latency * 1000,
        )

        # once the message is on the bus, we do not care about its fate.
        # it is the job of _deliver_message to handle delivery.
        asyncio.create_task(self._deliver_message(message))

    async def _deliver_message(self, message: Message) -> None:
        await asyncio.sleep(message.deliver_at - message.sent_at)

        # off the wire unconditionally, before we even look at the destination
        del self.messages[message.id]

        node: RaftNode | None = self.cluster.nodes.get(message.dst)
        # if a node dies down after message is on the bus, return
        if node is None or not node.alive:
            log.debug(
                "bus: msg %d n%d -> n%d dead letter",
                message.id,
                message.src,
                message.dst,
            )
            return
        # payload only — nodes never see envelopes
        node.handle_message(message.payload)

    def set_partition(self, partition: list[set[int]]) -> None:
        seen: set[int] = set()
        for group in partition:
            if seen & group:
                raise ValueError(f"node(s) {seen & group} appear in more than one group")
            seen |= group
        self.partition = partition
        log.info("bus: partition set to %s", partition)

    def fix_partition(self) -> None:
        self.partition = None
        log.info("bus: partition removed")

    def set_latency(self, min_latency: float, max_latency: float) -> None:
        if not 0 <= min_latency <= max_latency:
            raise ValueError("latency must satisfy 0 <= min <= max")
        self.latency_range = (min_latency, max_latency)

    def set_drop_rate(self, drop_rate: float) -> None:
        if not 0 <= drop_rate <= 1:
            raise ValueError("drop_rate must be in [0, 1]")
        self.drop_rate = drop_rate

    def get_messages(self) -> list[Message]:
        return list(self.messages.values())

    def _is_partitioned(self, src: int, dst: int) -> bool:
        if self.partition is None:
            return False
        for group in self.partition:
            if src in group:
                return dst not in group  # deliverable only within the same group
        return True  # src in no group so disjoint

    # return who else is in the cluster (so other nodes have visibility while asking for votes).
    # includes dead nodes on purpose: senders cannot know who is alive, and quorum
    # math counts members, not survivors.
    def peer_ids(self, exclude: int) -> set[int]:
        return set(self.cluster.nodes.keys()) - {exclude}
