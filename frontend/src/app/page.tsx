"use client"

import { Plus, Send, Pause, Scissors, RotateCcw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { ClusterView } from "@/components/ClusterView"
import { LogPanel } from "@/components/LogPanel"
import { mockSnapshot } from "@/mock"

export default function App() {
  const snapshot = mockSnapshot
  const leader = snapshot.nodes.find((n) => n.role === "leader")
  const alive = snapshot.nodes.filter((n) => n.alive).length
  const quorum = Math.floor(snapshot.nodes.length / 2) + 1

  return (
    <div className="mx-auto flex min-h-dvh max-w-4xl flex-col gap-3 p-4 text-sm">
      <header className="flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <h1 className="text-sm font-semibold tracking-tight">raft visualizer</h1>
          <span className="text-xs text-muted-foreground">
            backend not wired · mock data
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <Badge variant="outline" className="font-mono">
            term {leader?.term ?? "–"}
          </Badge>
          <Badge variant="outline" className="font-mono">
            {alive}/{snapshot.nodes.length} up · quorum {quorum}
          </Badge>
          {leader ? (
            <Badge className="font-mono">leader n{leader.id}</Badge>
          ) : (
            <Badge variant="muted">no leader</Badge>
          )}
        </div>
      </header>

      <div className="flex items-center gap-1.5">
        <Button>
          <Plus /> add node
        </Button>
        <Button variant="outline">
          <Send /> submit command
        </Button>
        <Separator orientation="vertical" className="h-5" />
        <Button variant="outline">
          <Scissors /> partition
        </Button>
        <Button variant="outline">
          <RotateCcw /> heal
        </Button>
        <Button variant="ghost" size="icon" aria-label="pause">
          <Pause />
        </Button>
        <div className="ml-auto flex items-center gap-2 text-[10px] text-muted-foreground">
          <span className="font-mono">latency 50–150ms</span>
          <span className="font-mono">drop 0%</span>
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
            <ClusterView snapshot={snapshot} />
          </CardContent>
        </Card>

        <Card className="md:col-span-2">
          <CardHeader>
            <CardTitle>nodes</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1.5 p-2">
            {snapshot.nodes.map((n) => (
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
                  <Button variant="ghost" size="icon" className="h-5 w-5" aria-label={n.alive ? "kill node" : "reboot node"}>
                    {n.alive ? <Scissors /> : <RotateCcw />}
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
          <LogPanel snapshot={snapshot} />
        </CardContent>
      </Card>
    </div>
  )
}
