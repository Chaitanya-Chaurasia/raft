import pytest

from cluster import Cluster


class StubBus:
    """A recording stand-in for MessageBus: nodes' sends land in .sent instead
    of the network. Lets handler tests assert on outgoing traffic directly."""

    def __init__(self, member_ids: set[int]):
        self.member_ids = set(member_ids)
        self.sent: list[tuple[int, int, object]] = []

    def send(self, src: int, dst: int, payload) -> None:
        self.sent.append((src, dst, payload))

    def peer_ids(self, exclude: int) -> set[int]:
        return self.member_ids - {exclude}

    def payloads_to(self, dst: int) -> list:
        return [p for _, d, p in self.sent if d == dst]


class FakeNode:
    """Delivery target for bus tests: records every payload handed to it."""

    def __init__(self, alive: bool = True):
        self.alive = alive
        self.received: list = []

    def handle_message(self, payload) -> None:
        self.received.append(payload)


@pytest.fixture
def stub_bus():
    return StubBus({0, 1, 2})


@pytest.fixture
async def cluster():
    c = Cluster()
    yield c
    for node in c.nodes.values():
        if node.alive:
            node.stop()
