import type { ClusterSnapshot, NodeSnapshot } from "@/mock"
import { cn } from "@/lib/utils"

function LogRow({ node, maxLen }: { node: NodeSnapshot; maxLen: number }) {
  return (
    <div className={cn("flex items-center gap-1.5", !node.alive && "opacity-40")}>
      <div className="w-8 shrink-0 font-mono text-[10px] text-muted-foreground">
        n{node.id}
      </div>
      <div className="flex gap-0.5">
        {Array.from({ length: maxLen }).map((_, i) => {
          const entry = node.log[i]
          const committed = entry && i < node.commitIndex
          return (
            <div
              key={i}
              title={entry ? `${i + 1}: ${entry.command} (t${entry.term})` : undefined}
              className={cn(
                "flex h-6 w-9 items-center justify-center rounded-sm border font-mono text-[9px]",
                !entry && "border-dashed border-border text-transparent",
                entry && committed && "border-accent bg-accent text-accent-foreground",
                entry && !committed && "border-border bg-background text-foreground",
              )}
            >
              {entry ? `t${entry.term}` : "·"}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export function LogPanel({ snapshot }: { snapshot: ClusterSnapshot }) {
  const maxLen = Math.max(...snapshot.nodes.map((n) => n.log.length), 1)
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-1.5">
        <div className="w-8 shrink-0" />
        <div className="flex gap-0.5">
          {Array.from({ length: maxLen }).map((_, i) => (
            <div
              key={i}
              className="w-9 text-center font-mono text-[9px] text-muted-foreground"
            >
              {i + 1}
            </div>
          ))}
        </div>
      </div>
      {snapshot.nodes.map((n) => (
        <LogRow key={n.id} node={n} maxLen={maxLen} />
      ))}
    </div>
  )
}
