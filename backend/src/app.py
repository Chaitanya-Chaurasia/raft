from fastapi import FastAPI
from nodes import Node

app = FastAPI()

@app.get("/")
def get_node_state():
    pass

@app.post("/nodes")
def create_node(node: Node):
    pass

@app.delete("/crash/{node_id}")
def crash_node(node_id: str):
    pass

@app.post("/reboot/{node_id}")
def reboot_node(node_id: str):
    pass

