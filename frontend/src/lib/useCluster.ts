"use client"

import { useEffect, useRef, useState } from "react"
import {
  adaptSnapshot,
  type ClusterSnapshot,
  WS_URL,
  type WireSnapshot,
} from "@/lib/api"

export function useCluster() {
  const [snapshot, setSnapshot] = useState<ClusterSnapshot | null>(null)
  const [connected, setConnected] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)

  useEffect(() => {
    let disposed = false
    let retryTimer: ReturnType<typeof setTimeout>

    function connect() {
      if (disposed) return
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onmessage = (event) => {
        const wire: WireSnapshot = JSON.parse(event.data)
        setSnapshot(adaptSnapshot(wire))
      }
      ws.onclose = () => {
        setConnected(false)
        // keep trying: the backend may simply not be up yet
        retryTimer = setTimeout(connect, 1000)
      }
      ws.onerror = () => ws.close()
    }

    connect()
    return () => {
      disposed = true
      clearTimeout(retryTimer)
      wsRef.current?.close()
    }
  }, [])

  return { snapshot, connected }
}
