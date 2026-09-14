// wire format (what the backend sends) + view types (what components render),
// plus the adapter between them and thin REST helpers.

export const API_BASE =
  process.env.NEXT_PUBLIC_RAFT_API ?? "http://localhost:8001"
export const WS_URL = API_BASE.replace(/^http/, "ws") + "/ws"

// ── view types (components render these) ──────────────────────────────────

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

// ── wire format (backend field names) ─────────────────────────────────────

interface WireNode {
  id: number
  alive: boolean
  role: Role
  term: number
  voted_for: number | null
  commit_idx: number
  log: LogEntry[]
}

interface WireMessage {
  id: number
  src: number
  dst: number
  type: MessageType
  sent_at: number
  deliver_at: number
}

export interface WireSnapshot {
  now: number
  nodes: WireNode[]
  messages: WireMessage[]
}

export function adaptSnapshot(wire: WireSnapshot): ClusterSnapshot {
  return {
    nodes: wire.nodes.map((n) => ({
      id: n.id,
      alive: n.alive,
      role: n.role,
      term: n.term,
      votedFor: n.voted_for,
      commitIndex: n.commit_idx,
      log: n.log,
    })),
    messages: wire.messages.map((m) => {
      const flight = m.deliver_at - m.sent_at
      const progress = flight > 0 ? (wire.now - m.sent_at) / flight : 1
      return {
        id: m.id,
        src: m.src,
        dst: m.dst,
        type: m.type,
        progress: Math.min(1, Math.max(0, progress)),
      }
    }),
  }
}

// ── REST helpers ───────────────────────────────────────────────────────────

async function post(path: string, body?: unknown) {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: body !== undefined ? { "content-type": "application/json" } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  return res.json()
}

export const api = {
  addNode: () => post("/nodes"),
  killNode: (id: number) => post(`/nodes/${id}/kill`),
  rebootNode: (id: number) => post(`/nodes/${id}/reboot`),
  submitCommand: (command: string) => post("/command", { command }),
  setPartition: (groups: number[][]) => post("/partition", { groups }),
  healPartition: () => post("/partition/heal"),
}
