import asyncio

import pytest

import nodes as nodes_module
from cluster import Cluster
from models import Role


@pytest.fixture
def fast_constants(monkeypatch):
    # ~10x faster than the interactive constants so the full lifecycle runs in ~3s.
    # the broadcastTime << electionTimeout hierarchy is preserved.
    monkeypatch.setattr(nodes_module, "ELECTION_TIMEOUT_RANGE", (0.15, 0.30))
    monkeypatch.setattr(nodes_module, "HEARTBEAT_INTERVAL", 0.05)


def leader_of(cluster: Cluster):
    leaders = [n for n in cluster.nodes.values() if n.alive and n.role == Role.LEADER]
    assert len(leaders) <= 1, f"two live leaders: {[n.id for n in leaders]}"
    return leaders[0] if leaders else None


async def test_full_lifecycle(fast_constants):
    cluster = Cluster()
    cluster.message_bus.latency_range = (0.005, 0.02)
    try:
        # spawn 3 -> exactly one leader emerges
        for _ in range(3):
            cluster.add_node()
        await asyncio.sleep(0.8)
        leader = leader_of(cluster)
        assert leader is not None, "no leader elected"

        # submit -> committed on every node
        assert cluster.submit_command("set x=1")["ok"]
        await asyncio.sleep(0.3)
        for n in cluster.nodes.values():
            assert n.commit_idx == 1, f"n{n.id} commit_idx={n.commit_idx}"

        # kill the leader -> survivors re-elect in a higher term
        old_leader_id = leader.id
        old_term = leader.current_term
        cluster.kill_node(old_leader_id)
        await asyncio.sleep(0.8)
        new_leader = leader_of(cluster)
        assert new_leader is not None, "no re-election"
        assert new_leader.id != old_leader_id
        assert new_leader.current_term > old_term

        # submit under the new reign -> commits on survivors
        assert cluster.submit_command("set y=2")["ok"]
        await asyncio.sleep(0.3)
        for n in cluster.nodes.values():
            if n.alive:
                assert n.commit_idx == 2

        # reboot the old leader -> catches up, does not reclaim leadership
        cluster.reboot_node(old_leader_id)
        await asyncio.sleep(0.6)
        rebooted = cluster.nodes[old_leader_id]
        assert rebooted.role != Role.LEADER
        assert rebooted.commit_idx == 2
        assert [e.command for e in rebooted.logs] == ["set x=1", "set y=2"]
    finally:
        for n in cluster.nodes.values():
            if n.alive:
                n.stop()


async def test_partition_and_heal(fast_constants):
    cluster = Cluster()
    cluster.message_bus.latency_range = (0.005, 0.02)
    try:
        for _ in range(3):
            cluster.add_node()
        await asyncio.sleep(0.8)
        leader = leader_of(cluster)
        assert leader is not None

        # cut the leader off from the majority -> majority side elects a new leader
        others = set(cluster.nodes) - {leader.id}
        cluster.message_bus.set_partition([{leader.id}, others])
        await asyncio.sleep(0.8)

        majority_leaders = [
            n for n in cluster.nodes.values() if n.id in others and n.role == Role.LEADER
        ]
        assert len(majority_leaders) == 1, "majority side failed to elect"
        assert leader.role == Role.LEADER, "minority leader cannot learn it was deposed yet"

        # heal -> old leader steps down within a beat or two
        cluster.message_bus.fix_partition()
        await asyncio.sleep(0.4)
        assert leader.role == Role.FOLLOWER
        assert leader_of(cluster).id in others
    finally:
        for n in cluster.nodes.values():
            if n.alive:
                n.stop()
