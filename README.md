# CECS 327 Assignment 4

Chord-based DFS with distributed sorting and Paxos replication.

## Requirements

- Python 3.12+
- PowerShell

## Run

Start 5 nodes in 5 separate terminals. Each takes `<node_id> <port> <data_dir>`:

```powershell
python .\main.py 2  5001 2
python .\main.py 4  5002 4
python .\main.py 6  5003 6
python .\main.py 8  5004 8
python .\main.py 10 5005 10
```

Each node starts a TCP server and joins the Chord ring. Node 2 (port 5001) is the
designated Paxos leader among server nodes. Data is stored under `data/node<id>/`.

## How to Issue DFS Commands

Run the client in a 6th terminal after all 5 nodes are up:

```powershell
python .\client_test.py
```

The client acts as a router node (ID 999). It issues commands through the Chord ring
using Paxos-protected writes. The demo is fully repeatable — it cleans up DFS state
from previous runs before starting.

## Demo Walkthrough

The client test runs the following sections automatically:

**Section 1 — Chord Ring Topology**
Prints the ring node order, ring size (256 slots, 8-bit), and replication factor (R=3).

**Section 2 — Finger Tables**
Prints the 8-entry finger table for every node in the ring, showing the start offset
and responsible successor for each finger.

**Section 3–7 — DFS Operations**
- `touch(dfs_music_input.csv)` — create empty DFS file via Paxos
- `append(dfs_music_input.csv, sample_input.csv)` — distribute file into pages via Paxos
- `stat(dfs_music_input.csv)` — print metadata including per-page replica placement
- `ls()` — list all DFS files
- `read(dfs_music_input.csv)` — reconstruct file from distributed pages

**Section 8–9 — Distributed Sort**
- `sort_file(dfs_music_input.csv, dfs_music_sorted.csv)` — partition records across nodes
  by sort key, collect and merge, write sorted output as a single Paxos-protected op
- Correctness check: verifies the output is globally sorted
- Saves sorted output locally to `dfs_music_sorted.csv`

**Section 10–11 — Paxos Replication Demo**
- Paxos-protected `touch` and `delete_file` with 3 replicas (nodes 2, 4, 6)
- Full PROPOSE → ACCEPT → LEARN → COMMIT message flow printed to terminal and logged

**Section 12 — Failure Scenario**
- Runs a Paxos round with 2 live replicas + 1 crashed node (port 9998, not running)
- Demonstrates that majority (2/3) is still reached despite one unreachable node
- The crashed node's ACCEPT fails gracefully; COMMIT still succeeds

## Paxos Log

Every Paxos message is appended to `paxos_log.txt` in the project root automatically.
The log includes timestamps, node role (LEADER/FOLLOWER), ballot number, message type
(PROPOSE/ACCEPT/LEARN/COMMIT/APPLY), and operation type for every round.

Example entries:
```
[12:01:05.123][PAXOS][LEADER][Node 999] PROPOSE ballot=1 op_type=touch (need 2/3 for majority)
[12:01:05.124][PAXOS][LEADER][Node 999] ACCEPT  -> Node 2 ballot=1
[12:01:05.131][PAXOS][FOLLOWER][Node 2] ACCEPT  ballot=1 op_type=touch -> ACK LEARN
[12:01:05.132][PAXOS][LEADER][Node 999] LEARN   <- Node 2 ballot=1 (ack)
[12:01:05.140][PAXOS][LEADER][Node 999] COMMIT  ballot=1 acks=3/3 -> COMMITTED
[12:01:05.141][PAXOS][LEADER][Node 999] APPLY   ballot=1 op_type=touch (leader applying)
```

## Replication

- Replication factor: R=3
- Strategy: successor-based placement on the Chord ring
- Both file metadata and page data are replicated
- Each page descriptor in metadata includes the list of replica node IDs

## Implemented DFS Operations

| Operation | Description |
|---|---|
| `touch(filename)` | Create an empty DFS file |
| `append(filename, local_path)` | Append a local file's contents as new pages |
| `read(filename)` | Reconstruct full file content from all pages |
| `stat(filename)` | Show metadata: size, page count, version, replica placement |
| `ls()` | List all DFS files |
| `delete_file(filename)` | Delete all pages and metadata for a file |
| `head(filename, n)` | Read first n lines |
| `tail(filename, n)` | Read last n lines |
| `sort_file(input, output)` | Distributed sort via Chord-partitioned buckets |

## Architecture

```
DFS Client CLI  (client_test.py)
      |
      v
DFS API         (dfs.py)       — touch/append/read/sort_file/delete
      |
      v
Replication + Paxos (paxos.py) — leader, ballot, ACCEPT, LEARN, majority commit
      |
      v
Chord Routing   (chord_node.py)— find_successor, finger_table, get_replica_peers
      |
      v
Local Storage   (storage.py)   — JSON objects on disk under data/node<id>/objects/
TCP Server      (server.py)    — handles PUT/GET/DELETE/SORT/PAXOS_ACCEPT/PAXOS_LEARN
```

## Fault Model

- Crash-stop failures only (no Byzantine behavior)
- Messages may be delayed, lost, or reordered
- Three replicas required; majority (2/3) sufficient to commit
- Leader is deterministic (lowest node ID); no dynamic re-election
