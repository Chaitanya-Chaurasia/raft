"use client"

import { Plus, Send, Scissors, RotateCcw, Unplug, Skull } from "lucide-react"
import { useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { ClusterView } from "@/components/ClusterView"
import { LogPanel } from "@/components/LogPanel"
import { api } from "@/lib/api"
import { useCluster } from "@/lib/useCluster"

export default function App() {
  const { snapshot, connected } = useCluster()
  const [command, setCommand] = useState("set x=1")
  const [partitioned, setPartitioned] = useState(false)

  const nodes = snapshot?.nodes ?? []
  const leader = nodes.find((n) => n.alive && n.role === "leader")
  const alive = nodes.filter((n) => n.alive).length
  const quorum = Math.floor(nodes.length / 2) + 1

  async function togglePartition() {
    if (partitioned) {
      await api.healPartition()
      setPartitioned(false)
    } else if (leader) {
      // classic demo: cut the leader off from everyone else
      const others = nodes.filter((n) => n.id !== leader.id).map((n) => n.id)
      await api.setPartition([[leader.id], others])
      setPartitioned(true)
    }
  }

  return (
    <div className="mx-auto flex min-h-dvh max-w-4xl flex-col gap-3 p-4 text-sm">
      <header className="flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <h1 className="text-sm font-semibold tracking-tight">raft visualizer</h1>
          <span className="text-xs text-muted-foreground">
            {connected ? "live" : "connecting to backend…"}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <Badge variant="outline" className="font-mono">
            term {leader?.term ?? "–"}
          </Badge>
          <Badge variant="outline" className="font-mono">
            {alive}/{nodes.length} up · quorum {nodes.length ? quorum : "–"}
          </Badge>
          {leader ? (
            <Badge className="font-mono">leader n{leader.id}</Badge>
          ) : (
            <Badge variant="muted">no leader</Badge>
          )}
        </div>
      </header>

      <div className="flex items-center gap-1.5">
        <Button onClick={() => api.addNode()}>
          <Plus /> add node
        </Button>
        <form
          className="flex items-center gap-1.5"
          onSubmit={(e) => {
            e.preventDefault()
            api.submitCommand(command)
          }}
        >
          <input
            value={command}
            onChange={(e) => setCommand(e.target.value)}
            className="h-7 w-32 rounded-md border border-border bg-background px-2 font-mono text-xs tracking-tight outline-none focus:border-foreground"
          />
          <Button type="submit" variant="outline" disabled={!leader}>
            <Send /> submit
          </Button>
        </form>
        <Separator orientation="vertical" className="h-5" />
        <Button
          variant="outline"
          onClick={togglePartition}
          disabled={!partitioned && !leader}
        >
          {partitioned ? (
            <>
              <RotateCcw /> heal
            </>
          ) : (
            <>
              <Unplug /> isolate leader
            </>
          )}
        </Button>
        <div className="ml-auto flex items-center gap-2 text-[10px] text-muted-foreground">
          <span className="font-mono">latency 50–150ms</span>
          <span className="font-mono">beat 500ms</span>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3 md:grid-cols-5">
        <Card className="md:col-span-3">
          <CardHeader>
            <CardTitle>cluster</CardTitle>
            <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
              <span className="inline-flex items-center gap-1">
                <span className="inline-block h-2 w-2 rounded-full bg-accent" />
                append
              </span>
              <span className="inline-flex items-center gap-1">
                <span className="inline-block h-2 w-2 rotate-45 border border-accent" />
                vote
              </span>
              <span className="inline-flex items-center gap-1">
                <span className="inline-block h-2 w-2 rounded-full border border-accent" />
                reply
              </span>
            </div>
          </CardHeader>
          <CardContent className="p-2">
            {nodes.length > 0 && snapshot ? (
              <ClusterView snapshot={snapshot} />
            ) : (
              <div className="flex h-64 items-center justify-center text-xs text-muted-foreground">
                {connected ? "no nodes yet — add one" : "waiting for backend…"}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="md:col-span-2">
          <CardHeader>
            <CardTitle>nodes</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1.5 p-2">
            {nodes.length === 0 && (
              <div className="py-8 text-center text-xs text-muted-foreground">–</div>
            )}
            {nodes.map((n) => (
              <div
                key={n.id}
                className="flex items-center justify-between rounded-md border border-border px-2 py-1.5"
              >
                <div className="flex items-center gap-2">
                  <span className="font-mono text-xs font-semibold">n{n.id}</span>
                  {n.alive ? (
                    n.role === "leader" ? (
                      <Badge className="font-mono">leader</Badge>
                    ) : (
                      <Badge variant="outline" className="font-mono">
                        {n.role}
                      </Badge>
                    )
                  ) : (
                    <Badge variant="muted" className="font-mono">
                      dead
                    </Badge>
                  )}
                </div>
                <div className="flex items-center gap-2 font-mono text-[10px] text-muted-foreground">
                  <span>t{n.term}</span>
                  <span>vf {n.votedFor ?? "–"}</span>
                  <span>ci {n.commitIndex}</span>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5"
                    aria-label={n.alive ? "kill node" : "reboot node"}
                    onClick={() =>
                      n.alive ? api.killNode(n.id) : api.rebootNode(n.id)
                    }
                  >
                    {n.alive ? <Skull /> : <RotateCcw />}
                  </Button>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>logs</CardTitle>
          <span className="text-[10px] text-muted-foreground">
            filled = committed · outlined = uncommitted · dashed = missing
          </span>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          {snapshot && nodes.length > 0 ? (
            <LogPanel snapshot={snapshot} />
          ) : (
            <div className="py-4 text-center text-xs text-muted-foreground">–</div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
