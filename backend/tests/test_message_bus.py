import asyncio

import pytest
from conftest import FakeNode

from cluster import Cluster
from models import RequestVote


def vote_payload(term: int = 1) -> RequestVote:
    return RequestVote(term=term, candidate_id=0, last_log_idx=0, last_log_term=0)


def make_bus(fakes: dict[int, FakeNode]):
    cluster = Cluster()
    cluster.nodes.update(fakes)
    bus = cluster.message_bus
    bus.latency_range = (0.001, 0.002)
    return bus


async def test_send_delivers_payload_after_latency():
    dst = FakeNode()
    bus = make_bus({0: FakeNode(), 1: dst})

    bus.send(0, 1, vote_payload())
    assert dst.received == []          # not yet: still in flight

    await asyncio.sleep(0.05)
    assert len(dst.received) == 1
    assert isinstance(dst.received[0], RequestVote)


async def test_delivery_hands_over_payload_not_envelope():
    dst = FakeNode()
    bus = make_bus({0: FakeNode(), 1: dst})
    payload = vote_payload()

    bus.send(0, 1, payload)
    await asyncio.sleep(0.05)
    assert dst.received[0] is payload


async def test_in_flight_tracking_empties_after_delivery():
    bus = make_bus({0: FakeNode(), 1: FakeNode()})
    bus.latency_range = (0.05, 0.05)

    bus.send(0, 1, vote_payload())
    assert len(bus.get_messages()) == 1

    await asyncio.sleep(0.1)
    assert bus.get_messages() == []


async def test_message_ids_are_unique_and_increasing():
    bus = make_bus({0: FakeNode(), 1: FakeNode()})
    bus.latency_range = (0.05, 0.05)
    for _ in range(3):
        bus.send(0, 1, vote_payload())
    ids = [m.id for m in bus.get_messages()]
    assert ids == sorted(set(ids))
    await asyncio.sleep(0.1)


async def test_drop_rate_one_drops_everything():
    dst = FakeNode()
    bus = make_bus({0: FakeNode(), 1: dst})
    bus.drop_rate = 1.0

    for _ in range(10):
        bus.send(0, 1, vote_payload())

    assert bus.get_messages() == []    # dropped at send time, never in flight
    await asyncio.sleep(0.05)
    assert dst.received == []


async def test_dead_node_swallows_message_silently():
    dst = FakeNode(alive=False)
    bus = make_bus({0: FakeNode(), 1: dst})

    bus.send(0, 1, vote_payload())
    await asyncio.sleep(0.05)
    assert dst.received == []
    assert bus.get_messages() == []    # dead letter still leaves the wire


async def test_unknown_destination_is_a_dead_letter():
    bus = make_bus({0: FakeNode()})
    bus.send(0, 99, vote_payload())
    await asyncio.sleep(0.05)
    assert bus.get_messages() == []


async def test_partition_blocks_cross_group_and_allows_within():
    a, b, c = FakeNode(), FakeNode(), FakeNode()
    bus = make_bus({0: a, 1: b, 2: c})
    bus.set_partition([{0, 1}, {2}])

    bus.send(0, 1, vote_payload())     # same group: goes through
    bus.send(0, 2, vote_payload())     # cross group: silently blocked
    await asyncio.sleep(0.05)

    assert len(b.received) == 1
    assert c.received == []


async def test_node_in_no_partition_group_is_isolated():
    a, b = FakeNode(), FakeNode()
    bus = make_bus({0: a, 1: b})
    bus.set_partition([{1}])

    bus.send(0, 1, vote_payload())
    await asyncio.sleep(0.05)
    assert b.received == []


async def test_fix_partition_restores_delivery():
    a, b = FakeNode(), FakeNode()
    bus = make_bus({0: a, 1: b})
    bus.set_partition([{0}, {1}])
    bus.fix_partition()

    bus.send(0, 1, vote_payload())
    await asyncio.sleep(0.05)
    assert len(b.received) == 1


def test_set_partition_rejects_overlapping_groups():
    bus = make_bus({0: FakeNode(), 1: FakeNode()})
    with pytest.raises(ValueError):
        bus.set_partition([{0, 1}, {1}])


def test_set_latency_rejects_bad_ranges():
    bus = make_bus({})
    with pytest.raises(ValueError):
        bus.set_latency(0.5, 0.1)
    with pytest.raises(ValueError):
        bus.set_latency(-0.1, 0.5)


def test_set_drop_rate_rejects_out_of_bounds():
    bus = make_bus({})
    with pytest.raises(ValueError):
        bus.set_drop_rate(1.5)
    with pytest.raises(ValueError):
        bus.set_drop_rate(-0.1)


def test_peer_ids_excludes_self_and_includes_dead_nodes():
    dead = FakeNode(alive=False)
    bus = make_bus({0: FakeNode(), 1: FakeNode(), 2: dead})
    assert bus.peer_ids(exclude=0) == {1, 2}
