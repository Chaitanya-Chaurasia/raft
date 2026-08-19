from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class LogEntry(BaseModel):
    term: int
    command: str


class Role(StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


# a raft node will send this message via the bus to request votes from other nodes.
# the other nodes will need to make a choice based on the recency (last log) and how
# consistent it has been (last log term)
class RequestVote(BaseModel):
    type: Literal["request_vote"] = "request_vote"
    term: int
    candidate_id: int
    last_log_idx: int
    last_log_term: int


# raft nodes will send a reply via the bus on whether they've elected the node as a leader or not
class RequestVoteReply(BaseModel):
    type: Literal["request_vote_reply"] = "request_vote_reply"
    term: int
    voter_id: int
    vote_granted: bool


# when we have to send entries across our cluster, let us consider this scenario:
# node A is leader in term 2, it appends SET x=1 at index 5 to its own log, sends AppendEntries but crashes before anything is delivered so only A has that entry.
# node B wins the election for term 3 (but it never saw index 5 - fine because that entry was never committed). a new client writes, and B appends SET y=9 at index 5 of its log.
# now the cluster contains two different index-5 entries: A has (term=2, SET x=1), B has (term=3, SET y=9). so A recovers as a follower.
# B replicates onward: it sends A an AppendEntries carrying the entry for index 6, with prev_log_index=5. The consistency question is that we want the (term, index) to not diverge
# and in this case, if we only checked the prev_log_idx on a new client write at index = 6, both have 5, and A = [..., SET x=1, <index 6] and B = [..., SET y=9, <index 6>].
# so we check both, the index and the term. A doesn't match the leader's term on index 5, so B will overwrite A's history.
# the leader_commit is the leader announcing what to commit at prev_log_idx + 1
class AppendEntries(BaseModel):
    type: Literal["append_entries"] = "append_entries"
    term: int
    leader_id: int
    prev_log_idx: int
    prev_log_term: int
    entries: list[LogEntry] = []
    leader_commit: int = 0


# once a log entry has been appended, the node will return the term of the new entry, its id (follower_id), status and the total size of the node's entries (match_idx)
class AppendEntriesReply(BaseModel):
    type: Literal["append_entries_reply"] = "append_entries_reply"
    term: int
    follower_id: int
    success: bool
    match_idx: int


# we do a union of all different models into one parent type, with type as the identifier
MessagePayload = Annotated[
    RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply,
    Field(discriminator="type"),
]
