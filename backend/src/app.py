import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from cluster import Cluster
from models import CommandBody

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(name)-12s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)

SNAPSHOT_INTERVAL = 0.1  # seconds between websocket pushes (~10 fps)

cluster = Cluster()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # no startup work because the cluster boots empty and nodes are spawned via POST
    # when app is tearing down, we need to stop all nodes, and yield triggers that.
    yield
    for node in cluster.nodes.values():
        if node.alive:
            node.stop()


app = FastAPI(title="raft visualizer", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/state")
async def get_state():
    return cluster.get_cluster_snapshot()


@app.post("/nodes", status_code=201)
async def create_node():
    return {"id": cluster.add_node()}


@app.post("/nodes/{node_id}/kill")
async def kill_node(node_id: int):
    if not cluster.kill_node(node_id):
        raise HTTPException(status_code=409, detail="node missing or already dead")
    return {"ok": True}


@app.post("/nodes/{node_id}/reboot")
async def reboot_node(node_id: int):
    if not cluster.reboot_node(node_id):
        raise HTTPException(status_code=409, detail="node missing or already alive")
    return {"ok": True}


@app.delete("/nodes/{node_id}")
async def delete_node(node_id: int):
    if not cluster.delete_node(node_id):
        raise HTTPException(status_code=404, detail="no such node")
    return {"ok": True}


@app.post("/command")
async def submit_command(body: CommandBody):
    # "ok" means accepted into the leader's log but NOT yet committed.
    # commitment shows up in /state a beat or two later as commit_idx advances.
    return cluster.submit_command(body.command)


@app.websocket("/ws")
async def state_stream(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(cluster.get_cluster_snapshot())
            await asyncio.sleep(SNAPSHOT_INTERVAL)
    except WebSocketDisconnect:
        pass
