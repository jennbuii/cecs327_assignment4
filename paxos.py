import threading

class PAXOS:
    def __init__(self, node_id, chord, replicas, apply_callback, is_leader=False):
        self.node_id = node_id
        self.chord = chord
        self.replicas = replicas
        self.apply_callback = apply_callback
        self.is_leader = is_leader
        self.lock = threading.Lock()
        self.next = 1
        self.accepted = {}
        self.committed = set()
        self.last_applied = 0

    def _apply_accepted(self):
        while (self.last_applied + 1) in self.committed:
            next_id = self.last_applied + 1
            op = self.accepted.get(next_id)
            if op is not None:
                self.apply_callback(op)
                self.last_applied = next_id
    
    def handle_accept(self, proposal_id, op):
        with self.lock:
            self.accepted[proposal_id] = op
        return {"status": "success", "action": "PAXOS_LEARN", "proposal_id": proposal_id}
    
    def handle_learn(self, proposal_id):
        with self.lock:
            if proposal_id not in self.accepted:
                return {"status": "error", "message": "Proposal ID not accepted"}
            self.committed.add(proposal_id)
            self.last_applied = max(self.last_applied, proposal_id)
        return {"status": "success", "action": "PAXOS_LEARN", "proposal_id": proposal_id}

    def propose(self, op):
        if not self.is_leader:
            return {"status": "error", "message": "Not the leader"}
        with self.lock:
            proposal_id = self.next
            self.next += 1
        learned = 0
        for peer in self.replicas:
            if peer["node_id"] == self.node_id:
                response = self.handle_accept(proposal_id, op)
            else:
                response = self.chord._send_request(peer, {"action": "PAXOS_ACCEPT", "proposal_id": proposal_id, "op": op})
            if response["status"] == "success" and response.get("action") == "PAXOS_LEARN" and response.get("proposal_id") == proposal_id:
                learned += 1
        if learned < (len(self.replicas) // 2) + 1:
            return {"status": "error", "message": "Failed to reach consensus"}
        for peer in self.replicas:
            self.chord._send_request(peer, {"action": "PAXOS_LEARN", "proposal_id": proposal_id, "op": op})
        if self.apply_callback:
            self.apply_callback(op)
        return {"status": "success", "message": "Consensus reached"}