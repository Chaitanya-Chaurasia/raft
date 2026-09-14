import asyncio

from conftest import StubBus

from models import (
    AppendEntries,
    AppendEntriesReply,
    LogEntry,
    RequestVote,
    RequestVoteReply,
    Role,
)
from nodes import RaftNode


def make_node(node_id: int = 0, members: set[int] = frozenset({0, 1, 2})) -> RaftNode:
    return RaftNode(node_id, StubBus(set(members)))


def entries(*terms: int) -> list[LogEntry]:
    return [LogEntry(term=t, command=f"cmd{i}") for i, t in enumerate(terms)]


async def test_start_and_stop_lifecycle():
    node = make_node()
    node.start()
    assert node.alive and node.role == Role.FOLLOWER and node._task is not None
    node.stop()
    assert not node.alive and node._task is None


async def test_stop_preserves_persistent_state():
    node = make_node()
    node.start()
    node.current_term = 7
    node.voted_for = 2
    node.logs = entries(1, 2)
    node.stop()
    assert node.current_term == 7
    assert node.voted_for == 2
    assert len(node.logs) == 2


async def test_reboot_resets_volatile_but_keeps_wal():
    node = make_node()
    node.start()
    node.role = Role.LEADER
    node.current_term = 3
    node.logs = entries(1, 3)
    node.stop()
    node.start()
    assert node.role == Role.FOLLOWER
    assert node.current_term == 3
    assert len(node.logs) == 2
    node.stop()


async def test_start_election_increments_term_and_votes_for_self():
    node = make_node()
    node._start_election()
    assert node.current_term == 1
    assert node.role == Role.CANDIDATE
    assert node.voted_for == node.id
    assert node.votes_received == {node.id}


async def test_start_election_requests_votes_from_all_peers():
    node = make_node()
    node.logs = entries(1, 1)
    node._start_election()
    requests = [(dst, p) for _, dst, p in node.bus.sent if isinstance(p, RequestVote)]
    assert {dst for dst, _ in requests} == {1, 2}
    for _, p in requests:
        assert p.term == 1
        assert p.last_log_idx == 2
        assert p.last_log_term == 1


async def test_majority_of_grants_wins_leadership():
    node = make_node()
    node._start_election()
    node.handle_message(RequestVoteReply(term=1, voter_id=1, vote_granted=True))
    assert node.role == Role.LEADER


async def test_refusals_do_not_win_leadership():
    node = make_node()
    node._start_election()
    node.handle_message(RequestVoteReply(term=1, voter_id=1, vote_granted=False))
    node.handle_message(RequestVoteReply(term=1, voter_id=2, vote_granted=False))
    assert node.role == Role.CANDIDATE


async def test_duplicate_grant_from_same_voter_counts_once():
    node = make_node(members={0, 1, 2, 3, 4})
    node._start_election()
    node.handle_message(RequestVoteReply(term=1, voter_id=1, vote_granted=True))
    node.handle_message(RequestVoteReply(term=1, voter_id=1, vote_granted=True))
    assert node.role == Role.CANDIDATE     # 2 of 5 is not a majority


async def test_stale_vote_reply_is_ignored():
    node = make_node()
    node._start_election()
    node._start_election()                  # now campaigning in term 2
    node.handle_message(RequestVoteReply(term=1, voter_id=1, vote_granted=True))
    assert node.role == Role.CANDIDATE


async def test_become_leader_initializes_replication_state():
    node = make_node()
    node.logs = entries(1, 1, 1)
    node._start_election()
    node.handle_message(RequestVoteReply(term=1, voter_id=1, vote_granted=True))
    assert node.next_idx == {1: 4, 2: 4}
    assert node.match_idx == {1: 0, 2: 0}
    assert node.heartbeat_due == 0.0


async def test_grants_vote_to_valid_candidate():
    node = make_node()
    node.handle_message(RequestVote(term=1, candidate_id=1, last_log_idx=0, last_log_term=0))
    reply = node.bus.payloads_to(1)[-1]
    assert reply.vote_granted is True
    assert node.voted_for == 1
    assert node.current_term == 1


async def test_refuses_stale_term_candidate():
    node = make_node()
    node.current_term = 5
    node.handle_message(RequestVote(term=3, candidate_id=1, last_log_idx=9, last_log_term=3))
    reply = node.bus.payloads_to(1)[-1]
    assert reply.vote_granted is False
    assert reply.term == 5


async def test_one_ballot_per_term():
    node = make_node()
    node.handle_message(RequestVote(term=1, candidate_id=1, last_log_idx=0, last_log_term=0))
    node.handle_message(RequestVote(term=1, candidate_id=2, last_log_idx=0, last_log_term=0))
    assert node.bus.payloads_to(1)[-1].vote_granted is True
    assert node.bus.payloads_to(2)[-1].vote_granted is False
    assert node.voted_for == 1


async def test_regrant_to_same_candidate_is_idempotent():
    node = make_node()
    for _ in range(2):
        node.handle_message(RequestVote(term=1, candidate_id=1, last_log_idx=0, last_log_term=0))
    assert all(r.vote_granted for r in node.bus.payloads_to(1))


async def test_refuses_candidate_with_shorter_log():
    node = make_node()
    node.logs = entries(1, 1)
    node.handle_message(RequestVote(term=2, candidate_id=1, last_log_idx=1, last_log_term=1))
    assert node.bus.payloads_to(1)[-1].vote_granted is False


async def test_refuses_candidate_with_older_last_term_despite_longer_log():
    node = make_node()
    node.logs = entries(1, 2)
    node.handle_message(RequestVote(term=3, candidate_id=1, last_log_idx=10, last_log_term=1))
    assert node.bus.payloads_to(1)[-1].vote_granted is False


async def test_fresh_ballot_after_term_change():
    node = make_node()
    node.handle_message(RequestVote(term=1, candidate_id=1, last_log_idx=0, last_log_term=0))
    node.handle_message(RequestVote(term=2, candidate_id=2, last_log_idx=0, last_log_term=0))
    assert node.bus.payloads_to(2)[-1].vote_granted is True
    assert node.voted_for == 2


async def test_higher_term_demotes_leader_to_follower():
    node = make_node()
    node.role = Role.LEADER
    node.current_term = 2
    node.handle_message(RequestVoteReply(term=5, voter_id=1, vote_granted=False))
    assert node.role == Role.FOLLOWER
    assert node.current_term == 5
    assert node.voted_for is None


async def test_equal_term_append_entries_makes_candidate_stand_down():
    node = make_node()
    node._start_election()
    node.handle_message(AppendEntries(term=1, leader_id=1, prev_log_idx=0, prev_log_term=0))
    assert node.role == Role.FOLLOWER
    assert node.current_term == 1


async def test_stale_leader_gets_refused_and_educated():
    node = make_node()
    node.current_term = 4
    node.logs = entries(1, 2)
    node.handle_message(AppendEntries(term=2, leader_id=1, prev_log_idx=0, prev_log_term=0))
    reply = node.bus.payloads_to(1)[-1]
    assert reply.success is False
    assert reply.term == 4
    assert len(node.logs) == 2             # nothing grafted


async def test_consistency_check_rejects_missing_anchor():
    node = make_node()
    node.handle_message(AppendEntries(
        term=1, leader_id=1, prev_log_idx=5, prev_log_term=1, entries=entries(1),
    ))
    reply = node.bus.payloads_to(1)[-1]
    assert reply.success is False
    assert node.logs == []                 # nothing grafted past a failed anchor


async def test_consistency_check_rejects_wrong_term_at_anchor():
    node = make_node()
    node.current_term = 3
    node.logs = entries(1, 2)
    node.handle_message(AppendEntries(
        term=3, leader_id=1, prev_log_idx=2, prev_log_term=3, entries=entries(3),
    ))
    assert node.bus.payloads_to(1)[-1].success is False


async def test_appends_new_entries_and_acks_match():
    node = make_node()
    node.handle_message(AppendEntries(
        term=1, leader_id=1, prev_log_idx=0, prev_log_term=0,
        entries=entries(1, 1), leader_commit=0,
    ))
    reply = node.bus.payloads_to(1)[-1]
    assert reply.success is True
    assert reply.match_idx == 2
    assert [e.term for e in node.logs] == [1, 1]


async def test_retransmission_is_idempotent():
    node = make_node()
    msg = AppendEntries(
        term=1, leader_id=1, prev_log_idx=0, prev_log_term=0, entries=entries(1, 1),
    )
    node.handle_message(msg)
    node.handle_message(msg)
    assert len(node.logs) == 2


async def test_conflicting_suffix_is_truncated_and_replaced():
    node = make_node()
    node.current_term = 3
    node.logs = entries(1, 2, 2)           # entries 2,3 are a dead reign's residue
    node.handle_message(AppendEntries(
        term=3, leader_id=1, prev_log_idx=1, prev_log_term=1,
        entries=entries(3, 3),
    ))
    assert [e.term for e in node.logs] == [1, 3, 3]


async def test_stale_retransmission_does_not_shorten_log():
    node = make_node()
    node.logs = entries(1, 1, 1, 1)
    node.handle_message(AppendEntries(
        term=1, leader_id=1, prev_log_idx=0, prev_log_term=0, entries=entries(1, 1),
    ))
    assert len(node.logs) == 4             # duplicate of a prefix must not truncate


async def test_commit_idx_follows_leader_commit_clamped_to_tail():
    node = make_node()
    node.handle_message(AppendEntries(
        term=1, leader_id=1, prev_log_idx=0, prev_log_term=0,
        entries=entries(1, 1), leader_commit=5,
    ))
    assert node.commit_idx == 2


async def test_commit_idx_never_retreats():
    node = make_node()
    node.logs = entries(1, 1, 1)
    node.commit_idx = 3
    node.handle_message(AppendEntries(
        term=1, leader_id=1, prev_log_idx=3, prev_log_term=1, leader_commit=1,
    ))
    assert node.commit_idx == 3


def make_leader(members: set[int] = frozenset({0, 1, 2})) -> RaftNode:
    node = make_node(0, members)
    node.role = Role.LEADER
    node.current_term = 1
    node.logs = entries(1, 1)
    node.next_idx = {p: 3 for p in node._peer_ids()}
    node.match_idx = {p: 0 for p in node._peer_ids()}
    return node


async def test_failed_ack_walks_next_idx_back():
    leader = make_leader()
    leader.handle_message(AppendEntriesReply(term=1, follower_id=1, success=False, match_idx=0))
    assert leader.next_idx[1] == 2
    assert leader.match_idx[1] == 0        # a rejection teaches nothing about their log


async def test_walk_back_floors_at_one():
    leader = make_leader()
    leader.next_idx[1] = 1
    leader.handle_message(AppendEntriesReply(term=1, follower_id=1, success=False, match_idx=0))
    assert leader.next_idx[1] == 1


async def test_successful_ack_advances_bookkeeping_and_commits():
    leader = make_leader()
    leader.handle_message(AppendEntriesReply(term=1, follower_id=1, success=True, match_idx=2))
    assert leader.match_idx[1] == 2
    assert leader.next_idx[1] == 3
    assert leader.commit_idx == 2          # leader + n1 = 2 of 3


async def test_stale_ack_cannot_drag_match_idx_backward():
    leader = make_leader()
    leader.match_idx[1] = 2
    leader.handle_message(AppendEntriesReply(term=1, follower_id=1, success=True, match_idx=1))
    assert leader.match_idx[1] == 2


async def test_minority_ack_does_not_commit():
    leader = make_leader(members={0, 1, 2, 3, 4})
    leader.handle_message(AppendEntriesReply(term=1, follower_id=1, success=True, match_idx=2))
    assert leader.commit_idx == 0          # leader + n1 = 2 of 5


async def test_own_term_guard_blocks_commit_of_inherited_entries():
    leader = make_leader()
    leader.current_term = 2                # re-elected; log entries are term 1
    leader.handle_message(AppendEntriesReply(term=2, follower_id=1, success=True, match_idx=2))
    assert leader.commit_idx == 0


async def test_reply_ignored_when_no_longer_leader():
    leader = make_leader()
    leader.role = Role.FOLLOWER
    leader.handle_message(AppendEntriesReply(term=1, follower_id=1, success=True, match_idx=2))
    assert leader.match_idx[1] == 0


async def test_heartbeat_anchors_each_peer_at_its_next_idx():
    leader = make_leader()
    leader.next_idx = {1: 3, 2: 2}
    leader._on_heartbeat_interval()
    beats = {dst: p for _, dst, p in leader.bus.sent if isinstance(p, AppendEntries)}
    assert beats[1].prev_log_idx == 2 and beats[1].entries == []
    assert beats[2].prev_log_idx == 1 and len(beats[2].entries) == 1


async def test_heartbeat_handles_unknown_peer_via_setdefault():
    leader = make_leader()
    leader.bus.member_ids.add(9)
    leader._on_heartbeat_interval()
    assert leader.next_idx[9] == 3


async def test_receive_command_appends_on_leader():
    leader = make_leader()
    assert leader.receive_command("set x=1") is True
    assert leader.logs[-1].command == "set x=1"
    assert leader.logs[-1].term == leader.current_term


async def test_receive_command_refused_by_follower():
    node = make_node()
    assert node.receive_command("set x=1") is False
    assert node.logs == []


async def test_snapshot_shape():
    node = make_node()
    node.logs = entries(1)
    snap = node.snapshot()
    assert snap["id"] == 0
    assert snap["role"] == "follower"
    assert snap["log"] == [{"term": 1, "command": "cmd0"}]
