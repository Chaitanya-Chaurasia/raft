# every node lives in a RaftNode class, and is only directly going to talk to other
# nodes via the message_bus (a network simulation over our cluster). every node is spawned
# in our cluster and we can have many different clusters.
from models import AppendEntriesReply
from models import MessagePayload
from models import AppendEntries, RequestVoteReply, LogEntry, RequestVote, Role
import asyncio
import logging
import random
from logging import Logger
from message_bus import MessageBus

log: Logger = logging.getLogger(__name__)

TICK = 0.01  # how often _run checks the clocks
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)
# this has to be <<< ELECTION_TIMEOUT_RANGE so we can simulate multiple heartbeats per election
HEARTBEAT_INTERVAL = 0.5

# few terminologies for pedagogical clarifications:
#   - term : this is a numeric system we use to keep track of a cluster's history. this is local and
#            made available to other nodes via self.current_term. every time a leader changes,
#            we increment the term n/w-wide. if nodes are not killed (which is production standard),
#            terms will not change for months.
#   - election : if a leader fails to send heartbeats, and we eventually pass on the election
#                deadline, we're in a new term and have to start an election for a new leader
#   - heartbeats : every leader has to let the other nodes in the cluster know that its still alive
#                  and still the leader. so every interval (which is constant), the leader sends an
#                  AppendEntries message via the bus so the nodes are consistent.


class RaftNode:
    def __init__(self, node_id: int, bus: MessageBus):
        self.id = node_id
        self.bus = bus
        self.current_term = 0
        self.voted_for: int | None = None

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

    # method to officially start a node. this node is now added to asyncio's task list and will
    # keep running, and based off its role, communicate via the bus
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

    def recieve_command(self, command: str) -> bool:
        # only the leaders can append
        if self.role != Role.LEADER or not self.alive:
            return False
        self.logs.append(LogEntry(term=self.current_term, command=command))
        log.info(
            "n%d: accepted command %r at idx %d (term %d)",
            self.id,
            command,
            self._last_log_idx(),
            self.current_term,
        )
        return True

    # we handle the different kinds of incoming messages from the bus here.
    def handle_message(self, msg: MessagePayload) -> None:

        if not self.alive:
            return

        # the universal term rule is that higher term wins, unconditionally, before dispatch.
        # this is the only place in the protocol where ANY message type changes our state.
        # if we start to lag behind we must leave leadership and announce an election.
        if msg.term > self.current_term:
            self._become_follower(msg.term)

        match msg:
            case RequestVote():
                self._on_request_vote(msg)
            case RequestVoteReply():
                self._on_request_vote_reply(msg)
            case AppendEntries():
                self._on_append_entries(msg)
            case AppendEntriesReply():
                self._on_append_entries_reply(msg)

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

    # only a leader can send a heartbeat
    # majority of these are going to be empty chunks because leader should keep on pinging
    def _on_heartbeat_interval(self):
        now = asyncio.get_running_loop().time()
        # of course, we need to reset the hearbeat interval before proceeding
        self.heartbeat_due = now + HEARTBEAT_INTERVAL

        for peer_id in self._peer_ids():
            # sanity in case we're missing an entry for any peer node
            # mostly for ui/ux needs
            self.next_idx.setdefault(peer_id, self._last_log_idx() + 1)

            # we refer to the optimistic appending index
            prev_idx = self.next_idx[peer_id] - 1

            # and we only want to send log entries starting at prev_idx of the follower
            # for ex: if a node is lagging at term 4, and we are at term 8, we send self.logs[4:]
            # so it can catch up.
            self.bus.send(
                self.id,
                peer_id,
                AppendEntries(
                    term=self.current_term,
                    leader_id=self.id,
                    prev_log_idx=prev_idx,
                    prev_log_term=self._term_at(prev_idx),
                    entries=self.logs[prev_idx:],
                    leader_commit=self.commit_idx,
                ),
            )

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

    def _become_leader(self):
        self.role = Role.LEADER

        # rebuild replication bookkeeping from scratch
        last = self._last_log_idx()
        self.next_idx = {p: last + 1 for p in self._peer_ids()}  # optimism: assume caught up
        self.match_idx = {p: 0 for p in self._peer_ids()}  # knowledge: none confirmed

        # heartbeat immediately because the first beat IS the victory announcement, and it
        # must land before anyone else's election fuse burns down. the _run loop
        # sees now >= 0.0 on its next tick and fires _on_heartbeat_interval.
        self.heartbeat_due = 0.0

        log.info(
            "n%d: elected LEADER of term %d (votes: %s)",
            self.id,
            self.current_term,
            sorted(self.votes_received),
        )

    def _become_follower(self, term: int):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        old_role = self.role
        self.role = Role.FOLLOWER
        self._reset_election_deadline()
        self.votes_received = set()
        if old_role != Role.FOLLOWER:
            log.info("n%d: %s -> follower, term=%d", self.id, old_role, self.current_term)

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
            src=self.id,
            dst=msg.candidate_id,
            payload=RequestVoteReply(term=self.current_term, voter_id=self.id, vote_granted=grant),
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
            cluster_size = len(self._peer_ids()) + 1
            # win by majority
            if len(self.votes_received) > cluster_size // 2:
                self._become_leader()

    # this is the handler for when a leader appends a new entry to itself and to the other nodes.
    # sent by the leader node.
    def _on_append_entries(self, msg: AppendEntries):
        if msg.term < self.current_term:
            # we send match_idx as 0 because it is essentially meaningless info if terms
            # do not match.
            self.bus.send(
                src=self.id,
                dst=msg.leader_id,
                payload=AppendEntriesReply(
                    term=self.current_term, follower_id=self.id, success=False, match_idx=0
                ),
            )

        # in case the terms are legit, we want to make sure we're still the follower and
        # reset election deadline
        self.role = Role.FOLLOWER
        self._reset_election_deadline()

        # if we're in the same term, there are still consistency checks we gotta make:
        #   - the last index we wrote our command to, on leader and node should be same, else
        #     we are essentially going to diverge by writing to different indices.
        #   - the last term we wrote our command to, on the leader and node should be same also.
        #     this is because the leader could send an AppendEntries message and die, which means
        #     the followers will re-elect and continue writing. when the ex-leader comes back, it
        #     would've diverged with the now new-leader when it sends an AppendEntries ping.
        if (
            msg.prev_log_idx > self._last_log_idx()
            or self._term_at(msg.prev_log_idx) != msg.prev_log_term
        ):
            self.bus.send(
                src=self.id,
                dst=msg.leader_id,
                payload=AppendEntriesReply(
                    term=self.current_term, follower_id=self.id, success=False, match_idx=0
                ),
            )

        for i, entry in enumerate(msg.entries):
            # since raft log entries are 1-based, we add a +1.
            append_idx = msg.prev_log_idx + i + 1

            # we want to check if our log's idx to append is occupied or not
            # if not, we skip this check and append the entry directly.
            if append_idx <= self._last_log_idx():
                # if it has been occupied, this is our neat little dedup logic
                # we don't care about the contents, but more about the (append_idx, term) pair
                if self._term_at(append_idx) == entry.term:
                    continue
                # if terms are different, we diverged, hence we truncate our list to delete
                # the mismatching entry, and eventually come out of the block and append
                self.logs = self.logs[: append_idx - 1]
            self.logs.append(entry)

        # now that we've added new entries, we need to change our commit index
        # i.e. how much of our entries are verified against the leader.
        # we simply choose the min of leader's or our last log index (in case we
        # had to truncate the list in the block above)
        if msg.leader_commit > self.commit_idx:
            self.commit_idx = min(msg.leader_commit, self._last_log_idx())

        # once we have appended, we send an ack across the n/w via the bus
        self.bus.send(
            src=self.id,
            dst=msg.leader_id,
            payload=AppendEntriesReply(
                term=self.current_term,
                follower_id=self.id,
                success=True,
                match_idx=msg.prev_log_idx + len(msg.entries),
            ),
        )

    # this is the handler for when a leader acks that other nodes have written
    # sent by the follower nodes
    def _on_append_entries_reply(self, msg: AppendEntriesReply):
        if self.role != Role.LEADER or msg.term != self.current_term:
            return

        f = msg.follower_id

        if not msg.success:
            # if the entry append failed, we need to reset the next_idx array
            # we take the max because raft is 1-indexed
            self.next_idx[f] = max(1, self.next_idx.get(f, 1) - 1)
            return

        # if msg was appended successfully, we want to increment the match_idx of that peer.
        self.match_idx[f] = max(self.match_idx.get(f, 0), msg.match_idx)
        self.next_idx[f] = self.match_idx[f] + 1

        # once we've resolved for 1 follower, we need to check across the cluster as well
        # so we can check if the majority of match_idxs have been updated, so we can update
        # our commit_idx. example: 5 nodes; leader with 6 entries, commit_idx = 5; entry 6 was just
        # appended and sent out via the heartbeat:
        #   after my append:      [6, 5, 5, 3, 0]   position 2 holds 5; majority has 5 so commit = 5
        #   n1's ack lands (→6):  [6, 6, 5, 3, 0]   but still 5. (only 2 of 5 hold entry 6)
        #   n2's ack lands (→6):  [6, 6, 6, 3, 0]   three of five hold it.

        # we will add self._last_log_idx() because self.match_idx only keeps track of peers not self
        indices = sorted(
            list(self.match_idx.values()) + [self._last_log_idx()],
            reverse=True
        )

        majority = indices[(len(self._peer_ids()) + 1) // 2]

        if majority > self.commit_idx and self._term_at(majority) == self.current_term:
            log.info("n%d: commit_idx %d -> %d", self.id, self.commit_idx, majority)
            self.commit_idx = majority


    def _last_log_term(self) -> int:
        return self.logs[-1].term if self.logs else 0

    def _term_at(self, idx: int) -> int:
        # raft is 1-indexed so idx 0 means "before any entry", whose term is 0
        return self.logs[idx - 1].term if idx > 0 else 0

    def _last_log_idx(self) -> int:
        return len(self.logs)

    def _reset_election_deadline(self):
        now = asyncio.get_running_loop().time()
        self.election_deadline = now + random.uniform(*ELECTION_TIMEOUT_RANGE)

    def _on_election_timeout(self):
        self._start_election()

    def _peer_ids(self) -> set[int]:
        return self.bus.peer_ids(exclude=self.id)

    # return the current state of our node
    def snapshot(self) -> dict:
        pass
