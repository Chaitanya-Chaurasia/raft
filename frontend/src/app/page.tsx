"use client"

import { Github, Plus, RotateCcw, Send, Skull, Unplug } from "lucide-react"
import { useEffect, useRef, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { InfoTip } from "@/components/ui/infotip"
import { Separator } from "@/components/ui/separator"
import { ClusterView } from "@/components/ClusterView"
import { LogPanel } from "@/components/LogPanel"
import { api } from "@/lib/api"
import { useCluster } from "@/lib/useCluster"

export default function App() {
  const { snapshot, connected } = useCluster()
  const [command, setCommand] = useState("set x=1")

  // network knobs: local strings while editing, seeded once from the server
  const [latMin, setLatMin] = useState("50")
  const [latMax, setLatMax] = useState("150")
  const [drop, setDrop] = useState("0")
  const seeded = useRef(false)
  useEffect(() => {
    if (snapshot && !seeded.current) {
      seeded.current = true
      setLatMin(String(Math.round(snapshot.config.latencyMinMs)))
      setLatMax(String(Math.round(snapshot.config.latencyMaxMs)))
      setDrop(String(Math.round(snapshot.config.dropRate * 100)))
    }
  }, [snapshot])

  const nodes = snapshot?.nodes ?? []
  const leader = nodes.find((n) => n.alive && n.role === "leader")
  const alive = nodes.filter((n) => n.alive).length
  const majority = Math.floor(nodes.length / 2) + 1
  const partitioned = snapshot?.config.partitioned ?? false

  function pushConfig() {
    api.setConfig({
      latencyMinMs: Number(latMin) || 0,
      latencyMaxMs: Number(latMax) || 0,
      dropRate: (Number(drop) || 0) / 100,
    })
  }

  async function togglePartition() {
    if (partitioned) {
      await api.healPartition()
    } else if (leader) {
      // classic demo: cut the leader off from everyone else
      const others = nodes.filter((n) => n.id !== leader.id).map((n) => n.id)
      await api.setPartition([[leader.id], others])
    }
  }

  const knobClass =
    "h-6 rounded border border-border bg-background px-1 text-center font-mono text-[10px] outline-none focus:border-foreground"

  return (
    <>
      <header className="fixed inset-x-0 top-0 z-40 border-b border-border bg-background/95 backdrop-blur">
        <div className="mx-auto flex h-12 max-w-6xl items-center justify-between gap-4 px-4">
          <h1 className="truncate text-xs font-semibold tracking-tight sm:text-sm">
            Raft Consensus Algorithm for Consistency and fault tolerance in
            distributed systems.
          </h1>
          <div className="flex shrink-0 items-center gap-1.5">
            {!connected && (
              <span className="text-[10px] text-muted-foreground">
                connecting…
              </span>
            )}
            <Badge variant="outline" className="gap-1 font-mono">
              term {leader?.term ?? "–"}
              <InfoTip text="raft's logical clock: it increments on every election attempt, and a higher term always wins an argument between nodes" />
            </Badge>
            <Badge variant="outline" className="gap-1 font-mono">
              {alive}/{nodes.length} up · majority{" "}
              {nodes.length ? majority : "–"}
              <InfoTip text="floor(n/2)+1 members must agree to elect a leader or commit an entry. dead nodes still count toward n — lose too many and the cluster stalls safely" />
            </Badge>
            {leader ? (
              <Badge className="font-mono">leader n{leader.id}</Badge>
            ) : (
              <Badge variant="muted">no leader</Badge>
            )}
          </div>
        </div>
      </header>

      <main className="mx-auto flex max-w-6xl flex-col gap-3 px-4 pb-16 pt-16 text-sm">
        <div className="flex flex-wrap items-center gap-1.5">
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
            <InfoTip text="sends a command to the leader. it lands in the leader's log first (outlined), then commits (filled) once a majority of nodes store it" />
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
          <InfoTip text="cuts the current leader off from everyone else. the majority side elects a new leader; the isolated one keeps believing it leads until healing lets it hear the higher term" />
          <div className="ml-auto flex items-center gap-1.5 text-[10px] text-muted-foreground">
            <span className="inline-flex items-center gap-1">
              lat
              <InfoTip text="every message on the simulated network is delayed by a random amount in this range — raft never assumes timely delivery" />
            </span>
            <input
              value={latMin}
              onChange={(e) => setLatMin(e.target.value)}
              onBlur={pushConfig}
              onKeyDown={(e) => e.key === "Enter" && e.currentTarget.blur()}
              className={`${knobClass} w-10`}
            />
            <span>–</span>
            <input
              value={latMax}
              onChange={(e) => setLatMax(e.target.value)}
              onBlur={pushConfig}
              onKeyDown={(e) => e.key === "Enter" && e.currentTarget.blur()}
              className={`${knobClass} w-10`}
            />
            <span>ms</span>
            <Separator orientation="vertical" className="h-4" />
            <span className="inline-flex items-center gap-1">
              drop
              <InfoTip text="chance that each message silently vanishes in flight. raft heals drops by re-sending state on every heartbeat — crank it up and watch things still converge" />
            </span>
            <input
              value={drop}
              onChange={(e) => setDrop(e.target.value)}
              onBlur={pushConfig}
              onKeyDown={(e) => e.key === "Enter" && e.currentTarget.blur()}
              className={`${knobClass} w-9`}
            />
            <span>%</span>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-3 md:grid-cols-5">
          <Card className="md:col-span-3">
            <CardHeader>
              <CardTitle className="inline-flex items-center gap-1.5">
                cluster
                <InfoTip text="nodes arranged in a ring; dots are rpcs in flight over the simulated network. the filled node is the leader pulsing heartbeats" />
              </CardTitle>
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
              <CardTitle className="inline-flex items-center gap-1.5">
                nodes
                <InfoTip text="t = current term · vf = who this node voted for in that term · ci = commit index, how much of its log is official" />
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-1.5 p-2">
              {nodes.length === 0 && (
                <div className="py-8 text-center text-xs text-muted-foreground">
                  –
                </div>
              )}
              {nodes.map((n) => (
                <div
                  key={n.id}
                  className="flex items-center justify-between rounded-md border border-border px-2 py-1.5"
                >
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs font-semibold">
                      n{n.id}
                    </span>
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
            <CardTitle className="inline-flex items-center gap-1.5">
              logs
              <InfoTip text="each row is one node's replicated log; each cell is one command labeled with the term that wrote it. rows converge to match the leader's row — that's the whole algorithm" />
            </CardTitle>
            <span className="text-[10px] text-muted-foreground">
              filled = committed · outlined = uncommitted · dashed = missing
            </span>
          </CardHeader>
          <CardContent className="overflow-x-auto">
            {snapshot && nodes.length > 0 ? (
              <LogPanel snapshot={snapshot} />
            ) : (
              <div className="py-4 text-center text-xs text-muted-foreground">
                –
              </div>
            )}
          </CardContent>
        </Card>
      </main>

      <footer className="fixed inset-x-0 bottom-0 z-40 border-t border-border bg-background">
        <div className="mx-auto flex h-9 max-w-6xl items-center justify-between px-4 text-[10px] tracking-tight text-muted-foreground">
          <span>built by chaitanya · raft from scratch</span>
          <a
            href="https://github.com/chaitanya/raft-visualizer"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 transition-colors hover:text-foreground"
          >
            <Github className="h-3 w-3" /> github
          </a>
        </div>
      </footer>
    </>
  )
}
