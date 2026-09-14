import asyncio

from message_bus import MessageBus
from nodes import RaftNode, Role

# these are simple operations based on how a network cluster would look like.
# CRUD node operations, send commands to leader, get cluster snapshot.


class Cluster:
    def __init__(self):
        self.nodes: dict[int, RaftNode] = {}
        self.message_bus = MessageBus(self)
        self.next_node_id = 0

    def add_node(self) -> int:
        node_id = self.next_node_id
        self.next_node_id += 1
        node = RaftNode(node_id, self.message_bus)
        self.nodes[node_id] = node
        node.start()
        return node_id

    def kill_node(self, node_id: int) -> bool:
        node = self.nodes.get(node_id)
        if node is None or not node.alive:
            return False
        node.stop()
        return True

    def reboot_node(self, node_id: int) -> bool:
        node = self.nodes.get(node_id)
        if node is None or node.alive:
            return False
        node.start()
        return True

    def delete_node(self, node_id: int) -> bool:
        node = self.nodes.get(node_id)
        if node is None:
            return False
        node.stop()
        del self.nodes[node_id]
        return True

    def submit_command(self, command: str) -> dict:
        for node in self.nodes.values():
            if node.alive and node.role == Role.LEADER and node.receive_command(command):
                return {"ok": True, "leader_id": node.id}
        return {"ok": False, "reason": "no_leader"}

    def get_cluster_snapshot(self) -> dict:
        return {
            # the bus's clock, so the frontend can place in-flight dots without
            # sharing a clock with the server
            "now": asyncio.get_running_loop().time(),
            "config": {
                "latency_min_ms": self.message_bus.latency_range[0] * 1000,
                "latency_max_ms": self.message_bus.latency_range[1] * 1000,
                "drop_rate": self.message_bus.drop_rate,
                "partitioned": self.message_bus.partition is not None,
            },
            "nodes": [node.snapshot() for node in self.nodes.values()],
            "messages": [
                {
                    "id": m.id,
                    "src": m.src,
                    "dst": m.dst,
                    "type": m.payload.type,
                    "sent_at": m.sent_at,
                    "deliver_at": m.deliver_at,
                }
                for m in self.message_bus.get_messages()
            ],
        }
