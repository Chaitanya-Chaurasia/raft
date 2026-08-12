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

    async def _run():
        pass

    def _on_election_timeout(self):
        pass
    
    def _on_heartbeat_interval(self):
        pass

    def _become_follower(self, term: int): pass-m "g"
    def _start_election(self): pass                # candidate's term+1, vote for self and ask everyone
    def _become_leader(self): pass

    def _on_request_vote(self, msg): ...          # decide: grant or refuse → send reply
    def _on_request_vote_reply(self, msg): ...    # count votes → maybe _become_leader
    def _on_append_entries(self, msg): ...        # consistency check, append, reply
    def _on_append_entries_reply(self, msg): ...  # update match/nextIndex → maybe advance commit

    # ══ output ══
    def snapshot(self) -> dict: ...               # my state, for Cluster.snapshot()
