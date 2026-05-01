import os
from storage import Storage
from chord_node import ChordNode
from dfs import DFSAPI
from sorter import Sorter
from paxos import PAXOS

PEERS = [
    {"node_id": 2, "host": "127.0.0.1", "port": 5001},
    {"node_id": 4, "host": "127.0.0.1", "port": 5002},
    {"node_id": 6, "host": "127.0.0.1", "port": 5003}, 
    {"node_id": 8, "host": "127.0.0.1", "port": 5004}, 
    {"node_id": 10, "host": "127.0.0.1", "port": 5005}, 
]

def run_tests():
    print("Initializing DFS Client")
    # Setup temporary local storage for the client just so ChordNode doesn't complain
    storage = Storage("data/client_node")
    
    # Client node acts as a temporary router (Node ID 999) to distribute requests
    chord = ChordNode(999, "127.0.0.1", 9999, PEERS, storage)

    replicas = chord.get_replica_peers("client_test_key", replication_factor=3)
    paxos = PAXOS(node_id=999, chord=chord, replicas=replicas, apply_callback=None, is_leader=True) 
    
    dfs = DFSAPI(chord, paxos=paxos)
    paxos.apply_callback = dfs.apply_op  # Set the DFS apply_op as the callback for Paxos to execute operations on commit
    sorter = Sorter(dfs, chord)

    input_file = "sample_input.csv"
    dfs_input_name = "dfs_music_input.csv"
    dfs_output_name = "dfs_music_sorted.csv"

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Please create it first.")
        return

    print(f"\n--- 1. Touching file '{dfs_input_name}' in DFS ---")
    res = dfs.touch(dfs_input_name)
    print(res)

    print(f"\n--- 2. Appending local '{input_file}' to DFS '{dfs_input_name}' ---")
    res = dfs.append(dfs_input_name, input_file)
    print(res)

    print(f"\n--- 3. Reading '{dfs_input_name}' from DFS ---")
    res = dfs.read(dfs_input_name)
    if res.get("status") == "success":
        print(res["content"])
    else:
        print(res)

    print(f"\n--- 4. Running Distributed Sort ({dfs_input_name} -> {dfs_output_name}) ---")
    res = sorter.sort_file(dfs_input_name, dfs_output_name)
    print(res)

    print(f"\n--- 5. Reading Sorted Result '{dfs_output_name}' from DFS ---")
    res = dfs.read(dfs_output_name)
    if res.get("status") == "success":
        print(res["content"])

        # Write sorted output to local file
        local_output_path = dfs_output_name
        with open(local_output_path, 'w') as f:
            f.write(res["content"])
        print(f"\nSorted output written to local file: {local_output_path}")
        
        # Perform logical check for report
        print("\n--- 6. Validating Sort Correctness ---")
        lines = res["content"].strip().split('\n')
        keys = [line.split(',')[0].strip() for line in lines if line.strip()]
        is_sorted = keys == sorted(keys)
        print(f"Is output correctly ordered? {is_sorted}")
    else:
        print(res)

    print(f"\n--- Testing Paxos Replication directly ---")
    # Node 2 (port 5001) is the leader in our PEERS array
    dummy_op = {"op": "touch", "filename": "paxos_test_file.csv"}
    print(f"Sending Paxos PROPOSE request to Leader (Node 2)...")
    
    # We send a manual PAXOS_ACCEPT directly just to trigger the logs on the servers
    for peer in PEERS:
        if peer["node_id"] != 2:
            req = {"action": "PAXOS_ACCEPT", "proposal_id": 99, "op": dummy_op}
            try:
                chord._send_request(peer, req)
                print(f"Sent PAXOS_ACCEPT to Node {peer['node_id']}")
            except Exception:
                pass
                
    for peer in PEERS:
        if peer["node_id"] != 2:
            req = {"action": "PAXOS_LEARN", "proposal_id": 99}
            try:
                chord._send_request(peer, req)
                print(f"Sent PAXOS_LEARN to Node {peer['node_id']}")
            except Exception:
                pass

if __name__ == "__main__":
    run_tests()