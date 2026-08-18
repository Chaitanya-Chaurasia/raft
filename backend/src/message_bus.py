from nodes import RaftNode
from cluster import Cluster
import asyncio
from random import random
import logging
from dataclasses import dataclass
from datetime import datetime
from models import MessagePayload

log = logging.getLogger(__name__)


@dataclass
class Message:
    id: int
    src: int
    dst: int
    payload: MessagePayload
    sent_at: datetime
    deliver_at: datetime


class MessageBus:
    def __init__(
        self,
        cluster: Cluster,
        latency_range: tuple[float, float] = (0.05, 0.15),
        drop_rate: float = 0.0,
        partition: list[set(int)] | None = None,
    ):
        self.messages: dict[int, Message] = {}
        self.cluster = cluster
        self.next_msg_id = 0
        # randomly generate a latency in this range (ms)
        self.latency_range: tuple[float, float] | None = latency_range or (0.05, 0.15)
        # a chance that the message vanishes (simulating some issue)
        self.drop_rate = drop_rate
        self.partition = partition

    async def send(self, message: Message):
        src, dst = message.src, message.dst
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
        msg = Message(
            id=self.next_msg_id,
            src=src,
            dst=dst,
            payload=message,
            sent_at=now,
            deliver_at=now + latency,
        )
        self.next_msg_id += 1
        self.messages[message.id] = msg

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

    async def _deliver_message(self, message: Message):
        await asyncio.sleep(message.deliver_at - message.sent_at)

        del self.messages[message.id]
        node: RaftNode = self.cluster.nodes.get[message.dst]
        # if a node dies down after message is on the bus, return
        if node is None or not node.alive:
            log.debug(
                  "bus: msg %d n%d -> n%d dead letter",
                  message.id, message.src, message.dst,
            )
            return
        node.handle_message(message)

    def set_partition(self, partition: list[set[int]]):
        pass

    def fix_partition(self):
        self.partition = None
        log.info("bus: partition removed")

    def set_latency(self, min_latency: int, max_latency: int):
        if not 0 <= min_latency <= max_latency:
            raise ValueError("latency must satisfy 0 <= min <= max")
        self.latency_range = (min_latency, max_latency)

    def set_drop_rate(self, drop_rate: float):
        if not 0 <= drop_rate <= 1:
            raise ValueError("drop_rate must be in [0, 1]")
        self.drop_rate = drop_rate

    def get_messages(self):
        return list(self.messages.values())

    def _is_partitioned(self, src: int, dst: int):
        if self.partition is None:
            return False
        for group in self.partition:
            if src in group:
                return dst not in group  # deliverable only within the same group
        return True  # src in no group so disjoint
