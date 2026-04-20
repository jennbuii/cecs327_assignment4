# Chord routing layer
import json
import socket
import hashlib

class ChordNode:
    def __init__(self, node_id, host, port, peers, storage, ring_bits=8):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers  # list of (host, port) tuples
        self.storage = storage
        self.ring_bits = ring_bits
        self.ring_size = 2 ** ring_bits

    #helper functions
    def _sorted_peers(self):
        return sorted(self.peers, key=lambda p: p["node_id"])
    
    def _hash(self, key):
        digest = hashlib.sha1(str(key).encode()).hexdigest()
        return int(digest, 16) % self.ring_size
    
    def _is_local(self, peer):
        return peer["node_id"] == self.node_id and peer["host"] == self.host and peer["port"] == self.port
    
    def find_successor_for_sort(self, sort_key):
        key_id = int(sort_key) % self.ring_size
        for peer in self._sorted_peers():
            if key_id <= peer["node_id"]:
                return peer
        return self._sorted_peers()[0]

    def find_successor(self, key):
        # find the peer responsible for the given key
        key_id = self._hash(key)
        sorted_peers = self._sorted_peers()
        for peer in sorted_peers:
            if key_id <= peer["node_id"]:
                return peer
        return sorted_peers[0]  # wrap around to the first peer
    
    def _send_request(self, peer, request):
        # send a request to the given peer and return the response
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((peer["host"], peer["port"]))
            s.sendall((json.dumps(request) + "\n").encode())
            data = b""
            while not data.endswith(b"\n"):
                response = s.recv(4096)
                if not response:
                    break
                data += response
        return json.loads(data.decode().strip())
    
    def put(self, key, obj):
        # store the object with the given key on the responsible peer
        peer = self.find_successor(key)
        if self._is_local(peer):
            self.storage.store_object(key, obj)
            return {"status": "success", "message": f"Object stored locally on node {self.node_id}."}
        else:
            request = {"action": "PUT", "key": key, "object": obj}
            response = self._send_request(peer, request)
            print(f"PUT request for key '{key}' owner={peer['node_id']}")
            return response
        
    def get(self, key):
        # retrieve the object with the given key from the responsible peer
        peer = self.find_successor(key)
        if self._is_local(peer):
            obj = self.storage.load_object(key)
            if obj is not None:
                return {"status": "success", "object": obj}
            else:
                return {"status": "error", "message": f"Object with key '{key}' not found on node {self.node_id}."}
        else:
            request = {"action": "GET", "key": key}
            response = self._send_request(peer, request)
            print(f"GET request for key '{key}' owner={peer['node_id']}")
            return response
        
    def delete(self, key):
        # delete the object with the given key from the responsible peer
        peer = self.find_successor(key)
        if self._is_local(peer):
            success = self.storage.delete_object(key)
            if success:
                return {"status": "success", "message": f"Object with key '{key}' deleted from node {self.node_id}."}
            else:
                return {"status": "error", "message": f"Object with key '{key}' not found on node {self.node_id}."}
        else:
            request = {"action": "DELETE", "key": key}
            response = self._send_request(peer, request)
            print(f"DELETE request for key '{key}' owner={peer['node_id']}")
            return response