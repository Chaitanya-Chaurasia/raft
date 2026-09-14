import pytest
from pydantic import TypeAdapter, ValidationError

from models import (
    AppendEntries,
    AppendEntriesReply,
    LogEntry,
    MessagePayload,
    RequestVote,
    RequestVoteReply,
)

payload_adapter = TypeAdapter(MessagePayload)


def test_log_entry_holds_term_and_command():
    entry = LogEntry(term=3, command="set x=1")
    assert entry.term == 3
    assert entry.command == "set x=1"


def test_request_vote_requires_all_fields():
    with pytest.raises(ValidationError):
        RequestVote(term=1, candidate_id=0)


def test_type_discriminators_are_set_automatically():
    assert (
        RequestVote(term=1, candidate_id=0, last_log_idx=0, last_log_term=0).type == "request_vote"
    )
    assert RequestVoteReply(term=1, voter_id=1, vote_granted=True).type == "request_vote_reply"
    assert (
        AppendEntries(term=1, leader_id=0, prev_log_idx=0, prev_log_term=0).type == "append_entries"
    )
    assert (
        AppendEntriesReply(term=1, follower_id=1, success=True, match_idx=0).type
        == "append_entries_reply"
    )


def test_discriminated_union_routes_dict_to_right_model():
    raw = {
        "type": "request_vote",
        "term": 2,
        "candidate_id": 1,
        "last_log_idx": 5,
        "last_log_term": 2,
    }
    parsed = payload_adapter.validate_python(raw)
    assert isinstance(parsed, RequestVote)
    assert parsed.last_log_idx == 5


def test_append_entries_defaults():
    msg = AppendEntries(term=1, leader_id=0, prev_log_idx=0, prev_log_term=0)
    assert msg.entries == []
    assert msg.leader_commit == 0


def test_entries_are_validated_log_entries():
    msg = AppendEntries(
        term=1,
        leader_id=0,
        prev_log_idx=0,
        prev_log_term=0,
        entries=[{"term": 1, "command": "set x=1"}],
    )
    assert isinstance(msg.entries[0], LogEntry)


def test_entries_default_is_not_shared_between_instances():
    a = AppendEntries(term=1, leader_id=0, prev_log_idx=0, prev_log_term=0)
    b = AppendEntries(term=1, leader_id=0, prev_log_idx=0, prev_log_term=0)
    a.entries.append(LogEntry(term=1, command="set x=1"))
    assert b.entries == []
