# raft visualizer

a raft implementation written from scratch, plus a ui that shows it running. the backend is a fastapi app that hosts a small cluster of virtual nodes in one process. nodes can only talk through a simulated message bus that adds random latency, drops messages, and can be partitioned, so leader elections and log replication play out the way they would on a real unreliable network. while it runs you can spawn nodes, crash and reboot them, submit commands, and cut the cluster in half to see what breaks.

the frontend subscribes to cluster snapshots over a websocket and draws the live state: who is leader, the current term, every rpc still in flight, and each node's log as entries replicate and commit. nothing here is meant to store real data. it exists to make the algorithm visible while learning how consensus actually behaves under failure.
