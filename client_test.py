import os
from datetime import datetime
from storage import Storage
from chord_node import ChordNode
from dfs import DFSAPI
from sorter import Sorter
from paxos import PAXOS, PAXOS_LOG_FILE

PEERS = [
    {"node_id": 2,  "host": "127.0.0.1", "port": 5001},
    {"node_id": 4,  "host": "127.0.0.1", "port": 5002},
    {"node_id": 6,  "host": "127.0.0.1", "port": 5003},
    {"node_id": 8,  "host": "127.0.0.1", "port": 5004},
    {"node_id": 10, "host": "127.0.0.1", "port": 5005},
]

# First 3 peers serve as the fixed Paxos replica set (deterministic, R=3)
PAXOS_REPLICAS = PEERS[:3]

def run_tests():
    # Write a run header to the log file so multiple runs are clearly separated
    with open(PAXOS_LOG_FILE, "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"  PAXOS LOG — run started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*60}\n")

    print("=" * 60)
    print("  DFS + Chord + Paxos Demo")
    print("=" * 60)

    # ----------------------------------------------------------------
    # Setup: client node (ID 999) acts as a router/leader — no server
    # ----------------------------------------------------------------
    storage = Storage("data/client_node")
    chord   = ChordNode(999, "127.0.0.1", 9999, PEERS, storage)
    paxos   = PAXOS(node_id=999, chord=chord, replicas=PAXOS_REPLICAS,
                    apply_callback=None, is_leader=True)
    dfs     = DFSAPI(chord, paxos=paxos)
    paxos.apply_callback = dfs.apply_op
    sorter  = Sorter(dfs, chord)

    # ----------------------------------------------------------------
    # Pre-demo cleanup: remove any DFS files left from a previous run
    # so touch() calls succeed cleanly on every run
    # ----------------------------------------------------------------
    for fname in ["dfs_music_input.csv", "dfs_music_sorted.csv",
                  "paxos_demo.csv", "fault_tolerance_test.csv"]:
        dfs._apply_delete_file(fname)  # no-op if the file doesn't exist

    # ----------------------------------------------------------------
    # Section 1: Chord ring topology + finger tables
    # ----------------------------------------------------------------
    print("\n--- 1. Chord Ring Topology ---")
    sorted_peers = chord._sorted_peers()
    ids = [str(p["node_id"]) for p in sorted_peers]
    print(f"  Ring order (node IDs): {' -> '.join(ids)} -> (wrap)")
    print(f"  Ring size: {chord.ring_size} (ring_bits={chord.ring_bits})")
    print(f"  Replication factor R=3 (successor-based placement)")

    print("\n--- 2. Finger Tables ---")
    for peer in sorted_peers:
        chord.print_finger_table(peer["node_id"])

    # ----------------------------------------------------------------
    # Section 2: DFS operations
    # ----------------------------------------------------------------
    input_file      = "sample_input.csv"
    dfs_input_name  = "dfs_music_input.csv"
    dfs_output_name = "dfs_music_sorted.csv"

    if not os.path.exists(input_file):
        print(f"\nError: {input_file} not found.")
        return

    print(f"\n--- 3. touch('{dfs_input_name}') ---")
    res = dfs.touch(dfs_input_name)
    print(res)

    print(f"\n--- 4. append('{dfs_input_name}', '{input_file}') ---")
    res = dfs.append(dfs_input_name, input_file)
    print(res)

    print(f"\n--- 5. stat('{dfs_input_name}') ---")
    res = dfs.stat(dfs_input_name)
    if res["status"] == "success":
        m = res["metadata"]
        print(f"  filename   : {m['filename']}")
        print(f"  size_bytes : {m['size_bytes']}")
        print(f"  num_pages  : {m['num_pages']}")
        print(f"  version    : {m['version']}")
        for p in m["pages"]:
            print(f"  page {p['page_no']}: key={p['page_key'][:12]}... "
                  f"size={p['size_bytes']} replicas={p.get('replicas', [])}")
    else:
        print(res)

    print(f"\n--- 6. ls() ---")
    res = dfs.ls()
    print(f"  Files: {res['filenames']}")

    print(f"\n--- 7. read('{dfs_input_name}') ---")
    res = dfs.read(dfs_input_name)
    if res["status"] == "success":
        lines = res["content"].splitlines()
        print(f"  Total lines: {len(lines)}")
        print(f"  First 3 lines: {lines[:3]}")
    else:
        print(res)

    # ----------------------------------------------------------------
    # Section 3: Distributed sort
    # ----------------------------------------------------------------
    print(f"\n--- 8. sort_file('{dfs_input_name}' -> '{dfs_output_name}') ---")
    res = sorter.sort_file(dfs_input_name, dfs_output_name)
    print(res)

    print(f"\n--- 9. Verify sort correctness ---")
    res = dfs.read(dfs_output_name)
    if res["status"] == "success":
        lines = res["content"].strip().split("\n")
        keys = [int(line.split(",")[0].strip()) for line in lines if line.strip()]
        is_sorted = keys == sorted(keys)
        print(f"  Total sorted lines : {len(lines)}")
        print(f"  First 3            : {lines[:3]}")
        print(f"  Last  3            : {lines[-3:]}")
        print(f"  Globally sorted?   : {is_sorted}")

        local_output = dfs_output_name
        with open(local_output, "w") as f:
            f.write(res["content"])
        print(f"  Saved locally to   : {local_output}")
    else:
        print(res)

    # ----------------------------------------------------------------
    # Section 4: Paxos replication demo (normal case, 3/3 replicas up)
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  Paxos Replication Demo")
    print("=" * 60)
    print(f"  Leader      : Node {paxos.node_id}")
    print(f"  Replicas    : {[p['node_id'] for p in PAXOS_REPLICAS]} (R=3)")
    print(f"  Majority    : {len(PAXOS_REPLICAS) // 2 + 1}/{len(PAXOS_REPLICAS)}")

    print(f"\n--- 10. Paxos-protected touch('paxos_demo.csv') ---")
    res = dfs.touch("paxos_demo.csv")
    print(f"  Result: {res}")

    print(f"\n--- 11. Paxos-protected delete('paxos_demo.csv') ---")
    res = dfs.delete_file("paxos_demo.csv")
    print(f"  Result: {res}")

    # ----------------------------------------------------------------
    # Section 5: Failure scenario — one replica crashes
    # ----------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  Failure Scenario: One Paxos Replica Crashes")
    print("=" * 60)

    dead_node = {"node_id": 99, "host": "127.0.0.1", "port": 9998}  # not running
    fault_replicas = [PEERS[0], PEERS[1], dead_node]  # 2 live + 1 dead
    fault_paxos = PAXOS(
        node_id=999, chord=chord, replicas=fault_replicas,
        apply_callback=dfs.apply_op, is_leader=True
    )

    print(f"\n  Replicas: {[p['node_id'] for p in fault_replicas]}")
    print(f"  Node {dead_node['node_id']} (port {dead_node['port']}) is CRASHED\n")

    op = {"op": "touch", "filename": "fault_tolerance_test.csv"}
    print(f"--- 12. Propose op with 1 crashed replica ---")
    result = fault_paxos.propose(op)
    print(f"\n  Final result: {result}")
    print(f"  (Majority of 2/3 reached despite one crashed node)")

    print(f"  Paxos log saved to: {PAXOS_LOG_FILE}")

if __name__ == "__main__":
    run_tests()
