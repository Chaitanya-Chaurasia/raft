from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Message:
    id: int
    src: int
    dst: int
    payload: MessagePayload
    sent_at: datetime
    deliver_at: datetime

class MessageBus:
    def __init__(self):
        self.messages: list[Message] = []

    async def send(self, message: Message):
        pass
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