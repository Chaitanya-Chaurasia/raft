from message_bus import MessageBus
from nodes import RaftNode

# in raft terms, a cluster is a set of nodes that are able to communicate with each other and agree on the same state.
# a node is a single instance of a raft server. (think of it as a single EC2 instance in an AWS region)
# in a real network, the nodes can talk to each other directly, but in our case, we're simulating a network
# where the nodes can only talk to the message bus. 

class Cluster:
    def __init__(self):
        self.nodes: dict[int: RaftNode] = {}
        self.message_bus = MessageBus()
        self.next_node_id = 0
    
    def add_node(self):
        pass

    # the difference between kill and delete is that kill is a temporary (still part of 
    # the cluster, should be fixed with a reboot) but delete is a permanent state.
    def kill_node(self, node_id: int):
        pass

    def delete_node(self, node_id: int):
        pass

    def get_cluster_snapshot(self, node_id: int):
        pass

    # since our raft nodes are not exposed to the API, we need to submit commands to the cluster
    # which will be forwarded to the appropriate node (based on the election algorithm)
    def submit_command(self, command: str):
        pass
