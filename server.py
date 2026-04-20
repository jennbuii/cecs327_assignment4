# handles TCP socket and threads
# opens a TCP socket, listens for connections, spawns a thread for each connection, and handles incoming messages
import json
import socket
import threading

class TCPServer:
    def __init__(self, host, port, storage):
        self.host = host
        self.port = port
        self.storage = storage
    
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