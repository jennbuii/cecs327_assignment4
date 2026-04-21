# CECS 327 Assignment 4

Chord-based DFS with distributed sorting and simplified Paxos replication.

## Requirements

- Python 3.12+
- PowerShell

## Run

Start 5 nodes in 5 separate terminals:

```powershell
python .\main.py 2 5001 2
python .\main.py 4 5002 4
python .\main.py 6 5003 6
python .\main.py 8 5004 8
python .\main.py 10 5005 10
```

Run the client test in a 6th terminal:

```powershell
python .\client_test.py
```

## What the client test does

1. Creates DFS input file (`touch`)
2. Appends `sample_input.csv` (`append`)
3. Reads input from DFS (`read`)
4. Runs distributed sort (`sort_file`)
5. Reads sorted DFS output
6. Validates order (`Is output correctly ordered? True`)
7. Sends sample Paxos `ACCEPT` and `LEARN` messages for logs

## Implemented DFS operations

- `touch(filename)`
- `append(filename, local_path)`
- `read(filename)`
- `head(filename, n)`
- `tail(filename, n)`
- `delete_file(filename)`
- `ls()`
- `stat(filename)`

## Notes

- Sort output file is created as a new DFS file.
- If the output filename already exists, use a new name or delete the old one first.