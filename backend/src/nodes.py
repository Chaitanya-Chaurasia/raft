# every node lives in a RaftNode class, and is only directly going to talk to other
# nodes via the message_bus (a network simulation over our cluster). every node is spawned
# in our cluster and we can have many different clusters.
from models import AppendEntries
from models import RequestVoteReply
import asyncio
import logging
import random
from logging import Logger
from message_bus import MessageBus
from models import LogEntry, RequestVote, Role

log: Logger = logging.getLogger(__name__)

TICK = 0.01  # how often _run checks the clocks
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)
# this has to be <<< ELECTION_TIMEOUT_RANGE so we can simulate multiple heartbeats per election
HEARTBEAT_INTERVAL = 0.5

class RaftNode:
    def __init__(self, node_id: int, bus: MessageBus):
        self.id = node_id
        self.bus = bus
        self.current_term = 0
        self.voted_for: int = 0

        # this is our persistent storage. on real nodes, this is implemented using WAL.
        self.logs: list[LogEntry] = []
        self.role: Role = Role.FOLLOWER
        self.alive = True
        self.commit_idx = 0
        self.election_deadline = 0.0

        # if this is a leader, we will be needing the following:
        #   - next_idx is a map of {node_id: idx} when the leader thinks optimistically i.e all
        #     nodes are on the same page as the leader
        #   - match_idx is a map of {node_id: idx} when the leader thinks pessimistically i.e, the
        #     highest index node has confirmed that matches the leader's logs
        self.next_idx: dict[int, int] = {}
        self.match_idx: dict[int, int] = {}
        self.heartbeat_due = 0.0

        self.votes_received: set[int] = set()
        self._task: asyncio.Task | None = None

    def start(self):
        self.alive = True
        self.role = Role.FOLLOWER
        # when a node restarts, it starts with a new deadline for electing a new leader in case
        # the leader does not send a heartbeat back.
        self._reset_election_deadline()
        self._task = asyncio.create_task(self._run())
        log.info("n%d: started, term=%d", self.id, self.current_term)

    def stop(self):
        self.alive = False
        if self._task is not None:
            self._task.cancel()
            self._task = None
        log.info("n%d: stopped (crash)", self.id)

    # the _run() method runs every interval to check for election timeouts and send heartbeats.
    async def _run(self):
        while self.alive:
            await asyncio.sleep(TICK)
            now = asyncio.get_running_loop().time()
            if self.role == Role.LEADER:
                if now >= self.heartbeat_due:
                    self._on_heartbeat_interval()
            elif now >= self.election_deadline:
                self._on_election_timeout()

    def _on_election_timeout(self):
        self._start_election()

    # only a leader can send a heartbeat
    # majority of these are going to be empty chunks because leader should keep on pinging
    def _on_heartbeat_interval(self):
        now = asyncio._get_running_loop().time()
        self.heartbeat_due = now + HEARTBEAT_INTERVAL

        for peer_id in self._peer_ids():
            self.bus.send(self.id, peer_id, AppendEntries(
                term=self.current_term,
                leader=self.id,
                prev_log_idx=self._last_log_index(),
                prev_log_term=self._last_log_term(),
                entries=[],
                leader_commit=self.commit_idx
            ))

    def _reset_election_deadline(self):
        now = asyncio.get_running_loop().time()
        self.election_deadline = now + random.uniform(*ELECTION_TIMEOUT_RANGE)

    def recieve_command():
        pass

    def handle_message():
        pass

    # in case a leader dies, we will increment the current term by 1
    # because elections can only happen in a new term:
    #   increment your current term
    #   vote for yourself
    #   reset election deadline to sometime in the future
    #   get the last log this node stored, and that logs last term (we will use this to check which
    #       node is most consistent with data)
    #   send a RequestVote message to all peers to request to be a leader.
    def _start_election(self):
        self.current_term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.id
        self.votes_received = {self.id}
        self._reset_election_deadline()

        log.info(
            "n%d: is trying to be a leader and hecne starting election for term %d",
            self.id,
            self.current_term,
        )

        last_idx = self._last_log_idx()
        last_term = self._last_log_term()

        peers = self._peer_ids()
        for peer_id in peers:
            self.bus.send(
                self.id,
                peer_id,
                RequestVote(
                    term=self.current_term,
                    candidate_id=self.id,
                    last_log_idx=last_idx,
                    last_log_term=last_term,
                ),
            )

    def _peer_ids(self) -> set[int]:
        return self.bus.peer_ids(exclude=self.id)

    def _become_leader(self):
        pass

    def _become_follower(self, term: int):
        self.role = Role.FOLLOWER


    # these are RPCs we will be sending to other nodes via the message bus
    # in a real world, this would be via gRPCs to other nodes
    # these are callbacks that will be called when _start_election or _become_leader are called.

    # this method is invoked when some other node doesn't hear a heartbeat from the leader
    # and hence requests a vote and you have to reply to the request.
    def _on_request_vote(self, msg: RequestVote):

        # if you're more consistent, do not say YES
        if msg.term < self.current_term:
            grant = False

        # only vote if you haven't voted already
        elif self.voted_for is not None and self.voted_for != msg.candidate_id:
            grant = False

        # check if candidate qualifies and has more logs
        elif (msg.last_log_term, msg.last_log_idx) < (self._last_log_term(), self._last_log_idx()):
            grant = False

        else:
            grant = True
            self.voted_for = msg.candidate_id
            # we want to reset election deadline because we just voted.
            # rule is simple - after a vote asked or given, reset the election deadline
            self._reset_election_deadline()

        self.bus.send(
            self.id, RequestVoteReply(term=self.current_term, voter_id=self.id, vote_granted=grant)
        )

    # this is the handler for node A for when node B, C, D sends a decision via _on_request_vote()
    def _on_request_vote_reply(self, msg: RequestVoteReply):
        # the tricky bit is that while we poll/wait for decisions, other nodes may also have
        # requested votes. first step is to check a stale request.
        if self.role != Role.CANDIDATE or msg.term != self.current_term:
            log.debug("Vote decision for n%d sent via n%d is stale", self.id, msg.voter_id)
            return
        if msg.vote_granted:
            self.votes_received.add(msg.voter_id)
            cluster_size = len(self._peer_ids())
            # win by majority
            if len(self.votes_received) > cluster_size // 2:
                self._become_leader()

    def _on_append_entries(self, msg):
        pass

    def _on_append_entries_reply(self, msg):
        pass

    def _last_log_term(self) -> int:
        return self.logs[-1].term if self.logs else 0

    def _last_log_idx(self) -> int:
      return len(self.logs)

    # return the current state of our node
    def snapshot(self) -> dict:
        pass
