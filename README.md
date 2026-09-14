# raft visualizer

a raft implementation written from scratch, plus a ui that shows it running. unless you're a stranger to distributed systems, you ought to know what partition tolerance is, and how raft/etcd is a consensus algorithm to achieve partition tolerance. i've attempted to simulate a cluster where nodes can only talk through a simulated message bus that adds random latency, drops messages, and can be partitioned, so leader elections and log replication play out the way they would on a real unreliable network. while it runs you can spawn nodes, crash and reboot them, submit commands, and cut the cluster in half to see what breaks.

i wanted to get my hands dirty so i did not use ai for this, purely based off this research paper: https://raft.github.io/raft.pdf

if you want to quickly get started, simply close and run `bash dev.sh`.
