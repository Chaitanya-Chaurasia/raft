// mirrors the shape the backend's Cluster.get_cluster_snapshot() will return.
// when the api exists, delete this file and fetch/subscribe instead.

export type Role = "leader" | "follower" | "candidate"

export interface LogEntry {
  term: number
  command: string
}

export interface NodeSnapshot {
  id: number
  alive: boolean
  role: Role
  term: number
  votedFor: number | null
  commitIndex: number
  log: LogEntry[]
}

export type MessageType =
  | "request_vote"
  | "request_vote_reply"
  | "append_entries"
  | "append_entries_reply"

export interface InFlightMessage {
  id: number
  src: number
  dst: number
  type: MessageType
  progress: number // 0..1 along the src -> dst path
}

export interface ClusterSnapshot {
  nodes: NodeSnapshot[]
  messages: InFlightMessage[]
}

export const mockSnapshot: ClusterSnapshot = {
  nodes: [
    {
      id: 0,
      alive: true,
      role: "leader",
      term: 3,
      votedFor: 0,
      commitIndex: 4,
      log: [
        { term: 1, command: "set x=1" },
        { term: 1, command: "set y=4" },
        { term: 2, command: "set x=7" },
        { term: 3, command: "del y" },
        { term: 3, command: "set z=2" },
        { term: 3, command: "set x=9" },
      ],
    },
    {
      id: 1,
      alive: true,
      role: "follower",
      term: 3,
      votedFor: 0,
      commitIndex: 4,
      log: [
        { term: 1, command: "set x=1" },
        { term: 1, command: "set y=4" },
        { term: 2, command: "set x=7" },
        { term: 3, command: "del y" },
        { term: 3, command: "set z=2" },
        { term: 3, command: "set x=9" },
      ],
    },
    {
      id: 2,
      alive: true,
      role: "follower",
      term: 3,
      votedFor: 0,
      commitIndex: 4,
      log: [
        { term: 1, command: "set x=1" },
        { term: 1, command: "set y=4" },
        { term: 2, command: "set x=7" },
        { term: 3, command: "del y" },
      ],
    },
    {
      id: 3,
      alive: false,
      role: "follower",
      term: 2,
      votedFor: null,
      commitIndex: 2,
      log: [
        { term: 1, command: "set x=1" },
        { term: 1, command: "set y=4" },
        { term: 2, command: "set x=7" },
      ],
    },
    {
      id: 4,
      alive: true,
      role: "follower",
      term: 3,
      votedFor: 0,
      commitIndex: 4,
      log: [
        { term: 1, command: "set x=1" },
        { term: 1, command: "set y=4" },
        { term: 2, command: "set x=7" },
        { term: 3, command: "del y" },
        { term: 3, command: "set z=2" },
      ],
    },
  ],
  messages: [
    { id: 101, src: 0, dst: 1, type: "append_entries", progress: 0.65 },
    { id: 102, src: 0, dst: 2, type: "append_entries", progress: 0.4 },
    { id: 103, src: 0, dst: 4, type: "append_entries", progress: 0.8 },
    { id: 104, src: 2, dst: 0, type: "append_entries_reply", progress: 0.25 },
    { id: 105, src: 4, dst: 0, type: "append_entries_reply", progress: 0.5 },
  ],
}
