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
        self.finger_table = self._build_finger_table(self.node_id)

    #helper functions
    def _sorted_peers(self):
        return sorted(self.peers, key=lambda p: p["node_id"])
    
    def _hash(self, key):
        digest = hashlib.sha1(str(key).encode()).hexdigest()
        return int(digest, 16) % self.ring_size
    
    def _is_local(self, peer):
        return peer["node_id"] == self.node_id and peer["host"] == self.host and peer["port"] == self.port

    def _find_successor_by_id(self, key_id):
        """Return the peer whose node_id is the successor of key_id on the ring."""
        for peer in self._sorted_peers():
            if key_id <= peer["node_id"]:
                return peer
        return self._sorted_peers()[0]

    def _build_finger_table(self, target_node_id):
        """Build a Chord finger table from the perspective of target_node_id."""
        table = []
        for i in range(self.ring_bits):
            start = (target_node_id + 2 ** i) % self.ring_size
            successor = self._find_successor_by_id(start)
            table.append({"finger": i + 1, "start": start, "successor_id": successor["node_id"]})
        return table

    def print_finger_table(self, target_node_id=None):
        """Print the finger table for this node (or any node ID if provided)."""
        nid = target_node_id if target_node_id is not None else self.node_id
        table = self._build_finger_table(nid) if target_node_id is not None else self.finger_table
        print(f"  Finger table for Node {nid}:")
        for entry in table:
            print(f"    [{entry['finger']:2d}] start={entry['start']:4d} -> Node {entry['successor_id']}")
    
    def find_successor_for_sort(self, sort_key):
        try:
            key_id = int(sort_key) % self.ring_size
        except ValueError:
            key_id = self._hash(sort_key)
            
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
    
    def get_replica_peers(self, key, replication_factor=3):
        sorted_peers = self._sorted_peers()
        try:
            key_id = int(key, 16) % self.ring_size
        except ValueError:
            key_id = self._hash(key)
        owner_index = 0
        for i, peer in enumerate(sorted_peers):
            if key_id <= peer["node_id"]:
                owner_index = i
                break
        else:
            owner_index = 0
        replicas = []
        for i in range(min(replication_factor, len(sorted_peers))):
            replicas.append(sorted_peers[(owner_index + i) % len(sorted_peers)])
        print(f"Key '{key}' (id={key_id}) is assigned to owner node {sorted_peers[owner_index]['node_id']} with replicas {[peer['node_id'] for peer in replicas]}")
        return replicas

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
        # store key val pair with replication on the responsible peer
        replicas = self.get_replica_peers(key)
        for peer in replicas:
            if self._is_local(peer):
                self.storage.store_object(key, obj)
            else:
                request = {"action": "PUT", "key": key, "object": obj}
                response = self._send_request(peer, request)
        return {"status": "success", "message": f"Object with key '{key}' stored on all replicas."}

    def get(self, key):
        # retrieve the object from the owner of the given key, then the replicas if the owner is unavailable
        replicas = self.get_replica_peers(key)
        for peer in replicas:
            try:
                if self._is_local(peer):
                    obj = self.storage.load_object(key)
                    if obj is not None:
                        return {"status": "success", "object": obj}
                else:
                    request = {"action": "GET", "key": key}
                    response = self._send_request(peer, request)
                    if response["status"] == "success":
                        return response
            except Exception as e:
                print(f"Error occurred while fetching key '{key}' from peer {peer['node_id']}: {e}")
        return {"status": "error", "message": f"Object with key '{key}' not found on any replica."}
    
    def delete(self, key):
        # delete the object with the given key from the responsible peer
        replicas = self.get_replica_peers(key)
        for peer in replicas:
            try:
                if self._is_local(peer):
                    success = self.storage.delete_object(key)
                else:
                    request = {"action": "DELETE", "key": key}
                    response = self._send_request(peer, request)
            except Exception as e:
                print(f"Error occurred while deleting key '{key}' from peer {peer['node_id']}: {e}")
        return {"status": "success", "message": f"Object with key '{key}' deleted from all replicas."}