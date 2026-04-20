# handles TCP socket and threads
# opens a TCP socket, listens for connections, spawns a thread for each connection, and handles incoming messages
import json
import socket
import threading
import os

class TCPServer:
    def __init__(self, host, port, storage):
        self.host = host
        self.port = port
        self.storage = storage
        self.base_dir = storage.base_dir
        self.partition_dir = os.path.join(self.base_dir, "sort_partitions")
        os.makedirs(self.partition_dir, exist_ok=True)
    
    def _partition_path(self, job_id):
        job_id_str = str(job_id).replace("/", "_")
        return os.path.join(self.partition_dir, f"{job_id_str}.json")
    
    def _save_partition(self, job_id, records):
        path = self._partition_path(job_id)
        with open(path, 'w') as f:
            json.dump(records, f)
    
    def _clear_partition(self, job_id):
        path = self._partition_path(job_id)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False
    
    def _load_partition(self, job_id):
        path = self._partition_path(job_id)
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
        return []

    def handle_client(self, conn):
        try:
            with conn:
                data = b""
                while not data.endswith(b"\n"):
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                request = json.loads(data.decode().strip())
                action = request.get("action")

                if action == "PUT":
                    key = request.get("key")
                    obj = request.get("object")
                    response = self.storage.store_object(key, obj)
                    response = {"status": "success"}
                    print(f"[{self.port}] handling {action} request for key '{key}'")
                
                elif action == "GET":
                    key = request.get("key")
                    obj = self.storage.load_object(key)
                    if obj is not None:
                        response = {"status": "success", "object": obj}
                    else:
                        response = {"status": "error", "message": f"Object with key '{key}' not found."}
                    print(f"[{self.port}] handling {action} request for key '{key}'")
                
                elif action == "DELETE":
                    key = request.get("key")
                    success = self.storage.delete_object(key)
                    if success:
                        response = {"status": "success"}
                    else:
                        response = {"status": "error", "message": f"Object with key '{key}' not found."}
                    print(f"[{self.port}] handling {action} request for key '{key}'")
                
                elif action == "ADD_SORT_PARTITION":
                    job_id = request.get("job_id")
                    record = request.get("record")
                    records = self._load_partition(job_id)
                    records.append(record)
                    self._save_partition(job_id, records)
                    response = {"status": "success"}
                    print(f"[{self.port}] handling {action} request for job_id '{job_id}'")

                elif action == "GET_SORT_PARTITION":
                    job_id = request.get("job_id")
                    records = self._load_partition(job_id)
                    if records is not None:
                        response = {"status": "success", "records": records}
                    else:
                        response = {"status": "error", "message": f"No partition found for job_id '{job_id}'."}
                    print(f"[{self.port}] handling {action} request for job_id '{job_id}'")

                elif action == "CLEAR_SORT_PARTITION":
                    job_id = request.get("job_id")
                    success = self._clear_partition(job_id)
                    if success:
                        response = {"status": "success"}
                    else:
                        response = {"status": "error", "message": f"No partition found for job_id '{job_id}'."}
                    print(f"[{self.port}] handling {action} request for job_id '{job_id}'")

                elif action == "PAXOS_ACCEPT":
                    proposal_id = request.get("proposal_id")
                    op = request.get("op")
                    response = self.storage.paxos.handle_accept(proposal_id, op)
                    print(f"[{self.port}] handling {action} request for proposal_id '{proposal_id}'")
                
                elif action == "PAXOS_LEARN":
                    proposal_id = request.get("proposal_id")
                    response = self.storage.paxos.handle_learn(proposal_id)
                    print(f"[{self.port}] handling {action} request for proposal_id '{proposal_id}'")

                else:
                    response = {"status": "error", "message": f"Unknown action '{action}'."}
                conn.sendall((json.dumps(response) + "\n").encode())
        
        except Exception as e:
            error_response = {"status": "error", "message": f"Server error: {str(e)}"}
            conn.sendall((json.dumps(error_response) + "\n").encode())
        finally:
            conn.close()
    
    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()
            print(f"Server listening on {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                print(f"Accepted connection from {addr}")
                threading.Thread(target=self.handle_client, args=(conn,)).start()