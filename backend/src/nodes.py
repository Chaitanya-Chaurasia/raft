# every node lives in a RaftNode class, and is only directly going to talk to other
# nodes via the message_bus (a network simulation over our cluster). every node is spawned
# in our cluster and we can have many different clusters.
class RaftNode:
    def __init__(self, value):
        pass

    def recieve_command():
        pass

    def handle_message():
        pass

    def start():
        pass

    def stop():
        pass

    # the _run() method runs every interval to check for election timeouts and send heartbeats.
    # on every interval finish, we increment the current term by 1, and we take the no. of terms
    # into account when selecting a new leader.
    async def _run():
        pass

    def _on_election_timeout(self):
        pass

    def _on_heartbeat_interval(self):
        pass

    def _become_follower(self, term: int):
        pass

    # in case a leader dies, we will increment the current term by 1
    # because elections can only happen in a new term.
    def _start_election(self):
        pass

    def _become_leader(self):
        pass

    # these are RPCs we will be sending to other nodes via the message bus
    # in a real world, this would be via gRPCs to other nodes
    # these are callbacks that will be called when _start_election or _become_leader are called.

    def _on_request_vote(self, msg):
        pass

    def _on_request_vote_reply(self, msg):
        pass

    def _on_append_entries(self, msg):
        pass

    def _on_append_entries_reply(self, msg):
        pass

    # return the current state of our node
    def snapshot(self) -> dict:
        pass
