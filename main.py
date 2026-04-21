# load config for node, creates ChordNode, creates DFS API object, starts TCP server
from storage import Storage
from chord_node import ChordNode
from dfs import DFSAPI
from server import TCPServer
from paxos import PAXOS
from sorter import Sorter
import sys

# Min 5 peers required
PEERS = [
    {"node_id": 2, "host": "127.0.0.1", "port": 5001},
    {"node_id": 4, "host": "127.0.0.1", "port": 5002},
    {"node_id": 6, "host": "127.0.0.1", "port": 5003}, 
    {"node_id": 8, "host": "127.0.0.1", "port": 5004}, 
    {"node_id": 10, "host": "127.0.0.1", "port": 5005}, 
]

def run_node(node_id, port, path_dir):
    storage = Storage(f"data/node{path_dir}")
    chord = ChordNode(node_id, "127.0.0.1", port, PEERS, storage)
    dfs = DFSAPI(chord)
    
    # Wire up Paxos
    is_leader = (node_id == PEERS[0]["node_id"])
    paxos = PAXOS(node_id, chord, PEERS, apply_callback=dfs.apply_op, is_leader=is_leader)
    storage.paxos = paxos
    dfs.paxos = paxos
    
    # Wire up Sorter
    sorter = Sorter(dfs, chord)

    server = TCPServer("127.0.0.1", port, storage)
    print(f"Node {node_id} running on port {port} with storage directory 'data/node{path_dir}'")
    server.start()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python main.py <node_id> <port> <path_dir>")
        sys.exit(1)
    node_id = int(sys.argv[1])  
    port = int(sys.argv[2])     
    dir_num = int(sys.argv[3])      
    run_node(node_id, port, dir_num)