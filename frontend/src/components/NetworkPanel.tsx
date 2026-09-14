"use client"

import { useEffect, useRef, useState } from "react"
import type { ClusterSnapshot, InFlightMessage, MessageType } from "@/lib/api"
import { cn } from "@/lib/utils"

interface DeliveredMessage {
  id: number
  src: number
  dst: number
  type: MessageType
}

const TYPE_LABEL: Record<MessageType, string> = {
  request_vote: "vote",
  request_vote_reply: "vote·re",
  append_entries: "append",
  append_entries_reply: "append·re",
}

function TypeGlyph({ type }: { type: MessageType }) {
  const reply = type.endsWith("_reply")
  const vote = type.startsWith("request_vote")
  return (
    <span
      className={cn(
        "inline-block h-2 w-2 shrink-0 border border-accent",
        vote ? "rotate-45" : "rounded-full",
        !reply && "bg-accent",
      )}
    />
  )
}

export function NetworkPanel({ snapshot }: { snapshot: ClusterSnapshot }) {
  // the wire is the stack: sends push onto it, deliveries pop off it.
  // we detect pops by diffing ids between consecutive snapshots.
  const prevRef = useRef<Map<number, InFlightMessage>>(new Map())
  const [delivered, setDelivered] = useState<DeliveredMessage[]>([])

  useEffect(() => {
    const current = new Map(snapshot.messages.map((m) => [m.id, m]))
    const popped: DeliveredMessage[] = []
    prevRef.current.forEach((m, id) => {
      if (!current.has(id)) {
        popped.push({ id, src: m.src, dst: m.dst, type: m.type })
      }
    })
    if (popped.length > 0) {
      setDelivered((old) => [...popped.reverse(), ...old].slice(0, 6))
    }
    prevRef.current = current
  }, [snapshot])

  const inFlight = [...snapshot.messages].sort((a, b) => b.id - a.id)

  return (
    <div className="space-y-2">
      <div>
        <div className="mb-1 font-mono text-[9px] uppercase tracking-wide text-muted-foreground">
          in flight · {inFlight.length}
        </div>
        {inFlight.length === 0 ? (
          <div className="rounded-md border border-dashed border-border py-2 text-center text-[10px] text-muted-foreground">
            wire is quiet
          </div>
        ) : (
          <div className="space-y-1">
            {inFlight.map((m) => (
              <div
                key={m.id}
                className="flex items-center gap-1.5 rounded-md border border-border px-1.5 py-1"
              >
                <TypeGlyph type={m.type} />
                <span className="w-16 shrink-0 font-mono text-[9px]">
                  {TYPE_LABEL[m.type]}
                </span>
                <span className="w-14 shrink-0 font-mono text-[9px] text-muted-foreground">
                  n{m.src} → n{m.dst}
                </span>
                <div className="h-1 flex-1 overflow-hidden rounded bg-muted">
                  <div
                    className="h-full bg-accent"
                    style={{ width: `${Math.round(m.progress * 100)}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {delivered.length > 0 && (
        <div>
          <div className="mb-1 font-mono text-[9px] uppercase tracking-wide text-muted-foreground">
            just delivered
          </div>
          <div className="space-y-1">
            {delivered.map((m, i) => (
              <div
                key={m.id}
                className="flex items-center gap-1.5 rounded-md px-1.5 py-0.5"
                style={{ opacity: 0.7 - i * 0.1 }}
              >
                <TypeGlyph type={m.type} />
                <span className="w-16 shrink-0 font-mono text-[9px]">
                  {TYPE_LABEL[m.type]}
                </span>
                <span className="font-mono text-[9px] text-muted-foreground">
                  n{m.src} → n{m.dst}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
