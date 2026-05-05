import threading
import os
from datetime import datetime

PAXOS_LOG_FILE = "paxos_log.txt"

class PAXOS:
    def __init__(self, node_id, chord, replicas, apply_callback, is_leader=False):
        self.node_id = node_id
        self.chord = chord
        self.replicas = replicas
        self.apply_callback = apply_callback
        self.is_leader = is_leader
        self.lock = threading.Lock()
        self.ballot = 1         # proposal/ballot number counter
        self.accepted = {}      # ballot -> op
        self.committed = set()  # set of committed ballot numbers

    def _log(self, msg):
        role = "LEADER" if self.is_leader else "FOLLOWER"
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        line = f"[{timestamp}][PAXOS][{role}][Node {self.node_id}] {msg}"
        print(line, flush=True)
        with open(PAXOS_LOG_FILE, "a") as f:
            f.write(line + "\n")

    def handle_accept(self, proposal_id, op):
        """Follower: store the proposed operation."""
        with self.lock:
            self.accepted[proposal_id] = op
        self._log(f"ACCEPT  ballot={proposal_id} op_type={op.get('op')} -> ACK LEARN")
        return {"status": "success", "action": "PAXOS_LEARN", "proposal_id": proposal_id}

    def handle_learn(self, proposal_id, op=None):
        """Follower: record that a proposal has been committed by the leader."""
        with self.lock:
            if op is not None and proposal_id not in self.accepted:
                self.accepted[proposal_id] = op
            if proposal_id not in self.accepted:
                return {"status": "error", "message": f"Proposal {proposal_id} not accepted"}
            self.committed.add(proposal_id)
            logged_op = self.accepted[proposal_id]
        self._log(f"LEARN   ballot={proposal_id} op_type={logged_op.get('op')} -> COMMITTED (logged)")
        return {"status": "success", "action": "PAXOS_LEARN", "proposal_id": proposal_id}

    def propose(self, op):
        """Leader: run one round of Paxos for the given operation."""
        if not self.is_leader:
            return {"status": "error", "message": "Not the leader"}
        with self.lock:
            proposal_id = self.ballot
            self.ballot += 1
        majority = (len(self.replicas) // 2) + 1
        self._log(
            f"PROPOSE ballot={proposal_id} op_type={op.get('op')} "
            f"(need {majority}/{len(self.replicas)} for majority)"
        )

        # Phase 1: Send ACCEPT to all replicas, collect LEARN acknowledgements
        learned = 0
        for peer in self.replicas:
            try:
                if peer["node_id"] == self.node_id:
                    response = self.handle_accept(proposal_id, op)
                else:
                    self._log(f"ACCEPT  -> Node {peer['node_id']} ballot={proposal_id}")
                    response = self.chord._send_request(
                        peer, {"action": "PAXOS_ACCEPT", "proposal_id": proposal_id, "op": op}
                    )
                    if response.get("status") == "success":
                        self._log(f"LEARN   <- Node {peer['node_id']} ballot={proposal_id} (ack)")
                if (response.get("status") == "success" and
                        response.get("action") == "PAXOS_LEARN" and
                        response.get("proposal_id") == proposal_id):
                    learned += 1
            except Exception as e:
                self._log(f"ACCEPT  -> Node {peer['node_id']} FAILED: {e} (node unreachable)")

        committed = learned >= majority
        self._log(
            f"COMMIT  ballot={proposal_id} acks={learned}/{len(self.replicas)} "
            f"-> {'COMMITTED' if committed else 'ABORTED (no majority)'}"
        )

        if not committed:
            return {
                "status": "error",
                "message": f"Failed to reach consensus (acks={learned}/{len(self.replicas)})"
            }

        # Phase 2: Broadcast LEARN to all replicas so they log the commitment
        # (leader applies directly below; skip sending LEARN to self)
        for peer in self.replicas:
            if peer["node_id"] != self.node_id:
                try:
                    self._log(f"LEARN   -> Node {peer['node_id']} ballot={proposal_id}")
                    self.chord._send_request(
                        peer, {"action": "PAXOS_LEARN", "proposal_id": proposal_id, "op": op}
                    )
                except Exception as e:
                    self._log(f"LEARN   -> Node {peer['node_id']} FAILED: {e}")

        # Leader applies the committed operation to the DFS state
        if self.apply_callback:
            self._log(f"APPLY   ballot={proposal_id} op_type={op.get('op')} (leader applying)")
            self.apply_callback(op)

        return {"status": "success", "message": f"Consensus reached (ballot={proposal_id})"}