import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from cluster import Cluster
from models import CommandBody, NetworkConfigBody, PartitionBody

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
    # any localhost port: next hops to 3001+ when 3000 is taken
    allow_origin_regex=r"http://localhost:\d+",
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


@app.post("/config")
async def set_network_config(body: NetworkConfigBody):
    try:
        cluster.message_bus.set_latency(body.latency_min_ms / 1000, body.latency_max_ms / 1000)
        cluster.message_bus.set_drop_rate(body.drop_rate)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    return {"ok": True}


@app.post("/partition")
async def set_partition(body: PartitionBody):
    try:
        cluster.message_bus.set_partition([set(g) for g in body.groups])
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    return {"ok": True}


@app.post("/partition/heal")
async def heal_partition():
    cluster.message_bus.fix_partition()
    return {"ok": True}


@app.websocket("/ws")
async def state_stream(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(cluster.get_cluster_snapshot())
            await asyncio.sleep(SNAPSHOT_INTERVAL)
    except WebSocketDisconnect:
        pass
