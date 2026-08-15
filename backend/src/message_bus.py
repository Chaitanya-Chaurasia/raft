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
        cluster,
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
              message.id, src, dst, latency * 1000,
        )
        asyncio.create_task(self._deliver_message(message))
    async def _deliver_message(self, message: Message):
        pass

    def set_partition(self, partition: list[int]):
        pass

    def fix_partition(self):
        pass

    def set_latency(self, latency: int):
        pass

    def set_drop_rate(self, drop_rate: float):
        pass

    def get_messages():
        pass

    def _is_partitioned(self, src: int, dst: int):
        if self.partition is None:
              return False
        for group in self.partition:
            if src in group:
                return dst not in group   # deliverable only within the same group
        return True   # src in no group so disjoint


