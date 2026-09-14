from models import Role
from nodes import RaftNode


async def test_add_node_assigns_sequential_ids_and_starts(cluster):
    assert cluster.add_node() == 0
    assert cluster.add_node() == 1
    assert all(n.alive for n in cluster.nodes.values())


async def test_node_ids_are_never_reused(cluster):
    cluster.add_node()
    cluster.add_node()
    cluster.delete_node(1)
    assert cluster.add_node() == 2


async def test_kill_stops_but_keeps_the_object(cluster):
    node_id = cluster.add_node()
    node = cluster.nodes[node_id]
    assert cluster.kill_node(node_id) is True
    assert not node.alive
    assert cluster.nodes[node_id] is node


async def test_kill_rejects_missing_or_dead(cluster):
    assert cluster.kill_node(99) is False
    node_id = cluster.add_node()
    cluster.kill_node(node_id)
    assert cluster.kill_node(node_id) is False


async def test_reboot_same_object_with_persistent_state(cluster):
    node_id = cluster.add_node()
    node = cluster.nodes[node_id]
    node.current_term = 5
    cluster.kill_node(node_id)
    assert cluster.reboot_node(node_id) is True
    assert cluster.nodes[node_id] is node
    assert node.alive
    assert node.current_term == 5


async def test_reboot_rejects_living_or_missing(cluster):
    node_id = cluster.add_node()
    assert cluster.reboot_node(node_id) is False
    assert cluster.reboot_node(99) is False


async def test_delete_removes_from_membership(cluster):
    node_id = cluster.add_node()
    assert cluster.delete_node(node_id) is True
    assert node_id not in cluster.nodes
    assert cluster.delete_node(node_id) is False


async def test_submit_without_leader_reports_no_leader(cluster):
    cluster.add_node()
    result = cluster.submit_command("set x=1")
    assert result == {"ok": False, "reason": "no_leader"}


async def test_submit_routes_to_the_leader(cluster):
    cluster.add_node()
    cluster.add_node()
    leader = cluster.nodes[0]
    leader.role = Role.LEADER
    result = cluster.submit_command("set x=1")
    assert result == {"ok": True, "leader_id": 0}
    assert leader.logs[-1].command == "set x=1"
    assert cluster.nodes[1].logs == []


async def test_snapshot_includes_nodes_and_in_flight_messages(cluster):
    cluster.add_node()
    cluster.add_node()
    snap = cluster.get_cluster_snapshot()
    assert {n["id"] for n in snap["nodes"]} == {0, 1}
    assert isinstance(snap["messages"], list)
    for key in ("id", "src", "dst", "type", "sent_at", "deliver_at"):
        for m in snap["messages"]:
            assert key in m


async def test_bus_and_cluster_are_mutually_wired(cluster):
    assert cluster.message_bus.cluster is cluster
    node_id = cluster.add_node()
    assert isinstance(cluster.nodes[node_id], RaftNode)
    assert cluster.message_bus.peer_ids(exclude=node_id) == set()


async def test_set_timing_applies_to_existing_and_future_nodes(cluster):
    first = cluster.add_node()
    cluster.set_timing(heartbeat_s=0.2, election_min_s=0.5, election_max_s=1.0)
    second = cluster.add_node()
    for node_id in (first, second):
        node = cluster.nodes[node_id]
        assert node.heartbeat_interval == 0.2
        assert node.election_timeout_range == (0.5, 1.0)


async def test_set_timing_enforces_the_hierarchy(cluster):
    import pytest

    with pytest.raises(ValueError):
        cluster.set_timing(heartbeat_s=0, election_min_s=1, election_max_s=2)
    with pytest.raises(ValueError):
        cluster.set_timing(heartbeat_s=1, election_min_s=1.5, election_max_s=2)
    with pytest.raises(ValueError):
        cluster.set_timing(heartbeat_s=0.1, election_min_s=2, election_max_s=1)
