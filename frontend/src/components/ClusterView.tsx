import type { ClusterSnapshot, InFlightMessage, NodeSnapshot } from "@/mock"

const W = 460
const H = 340
const CX = W / 2
const CY = H / 2
const RING = 118
const NODE_R = 26

function nodePos(index: number, total: number) {
  const angle = (index / total) * 2 * Math.PI - Math.PI / 2
  return { x: CX + RING * Math.cos(angle), y: CY + RING * Math.sin(angle) }
}

function MessageDot({
  msg,
  positions,
}: {
  msg: InFlightMessage
  positions: Map<number, { x: number; y: number }>
}) {
  const a = positions.get(msg.src)
  const b = positions.get(msg.dst)
  if (!a || !b) return null
  const x = a.x + (b.x - a.x) * msg.progress
  const y = a.y + (b.y - a.y) * msg.progress
  const isReply = msg.type.endsWith("_reply")
  const isVote = msg.type.startsWith("request_vote")
  return (
    <g>
      {isVote ? (
        <rect
          x={x - 3}
          y={y - 3}
          width={6}
          height={6}
          transform={`rotate(45 ${x} ${y})`}
          fill={isReply ? "#ffffff" : "#0a0a0a"}
          stroke="#0a0a0a"
          strokeWidth={1}
        />
      ) : (
        <circle
          cx={x}
          cy={y}
          r={3.5}
          fill={isReply ? "#ffffff" : "#0a0a0a"}
          stroke="#0a0a0a"
          strokeWidth={1}
        />
      )}
    </g>
  )
}

function NodeCircle({
  node,
  pos,
}: {
  node: NodeSnapshot
  pos: { x: number; y: number }
}) {
  const dead = !node.alive
  const leader = node.role === "leader"
  const candidate = node.role === "candidate"
  return (
    <g opacity={dead ? 0.35 : 1}>
      <circle
        cx={pos.x}
        cy={pos.y}
        r={NODE_R}
        fill={leader ? "#0a0a0a" : "#ffffff"}
        stroke="#0a0a0a"
        strokeWidth={1.25}
        strokeDasharray={candidate ? "4 3" : dead ? "2 3" : undefined}
      />
      <text
        x={pos.x}
        y={pos.y - 2}
        textAnchor="middle"
        fontSize={11}
        fontWeight={600}
        fill={leader ? "#ffffff" : "#0a0a0a"}
        className="font-mono"
      >
        n{node.id}
      </text>
      <text
        x={pos.x}
        y={pos.y + 10}
        textAnchor="middle"
        fontSize={8}
        fill={leader ? "#a3a3a3" : "#737373"}
        className="font-mono"
      >
        t{node.term}
      </text>
      <text
        x={pos.x}
        y={pos.y + NODE_R + 12}
        textAnchor="middle"
        fontSize={9}
        fill="#737373"
      >
        {dead ? "dead" : node.role}
      </text>
    </g>
  )
}

export function ClusterView({ snapshot }: { snapshot: ClusterSnapshot }) {
  const positions = new Map(
    snapshot.nodes.map((n, i) => [n.id, nodePos(i, snapshot.nodes.length)]),
  )
  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className="h-auto w-full"
      role="img"
      aria-label="raft cluster"
    >
      {/* faint links between every pair */}
      {snapshot.nodes.map((a, i) =>
        snapshot.nodes.slice(i + 1).map((b) => {
          const pa = positions.get(a.id)!
          const pb = positions.get(b.id)!
          return (
            <line
              key={`${a.id}-${b.id}`}
              x1={pa.x}
              y1={pa.y}
              x2={pb.x}
              y2={pb.y}
              stroke="#e5e5e5"
              strokeWidth={1}
            />
          )
        }),
      )}
      {snapshot.messages.map((m) => (
        <MessageDot key={m.id} msg={m} positions={positions} />
      ))}
      {snapshot.nodes.map((n) => (
        <NodeCircle key={n.id} node={n} pos={positions.get(n.id)!} />
      ))}
    </svg>
  )
}
