import os
import hashlib
import json

class Sorter:
    def __init__(self, dfs, chord):
        self.dfs = dfs
        self.chord = chord

    def parse_record(self, data):
        records = []
        for lines in data.splitlines():
            line = lines.strip()
            if not line:
                continue
            parts = line.split(",", 1)
            key = parts[0].strip()
            value = parts[1].strip()
            records.append((key, value))
        return records
    
    def _clear_partition(self, job_id):
        for peer in self.chord._sorted_peers():
            if self.chord._is_local(peer):
                partition_dir = os.path.join(self.chord.storage.base_dir, "sort_partitions")
                path = os.path.join(partition_dir, f"{job_id}.json")
                if os.path.exists(path):
                    os.remove(path)
                    print(f"Cleared partition for job_id '{job_id}' on node {peer['node_id']}")
            else:
                request = {"action": "CLEAR_SORT_PARTITION", "job_id": job_id}
                self.chord._send_request(peer, request)
                print(f"Sent CLEAR_PARTITION request for job_id '{job_id}' to node {peer['node_id']}")

    def distribute_partition(self, job_id, record):
        for key, value in record:
            peer = self.chord.find_successor_for_sort(key)
            record_data = [key, value]
            if self.chord._is_local(peer):
                partition_dir = os.path.join(self.chord.storage.base_dir, "sort_partitions")
                os.makedirs(partition_dir, exist_ok=True)
                path = os.path.join(partition_dir, f"{job_id}.json")
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        existing_records = json.load(f)
                else:
                    existing_records = []
                existing_records.append(record_data)
                with open(path, 'w') as f:
                    json.dump(existing_records, f)
                print(f"Stored local partition for job_id '{job_id}' to node {peer['node_id']} locally")
            else:
                response = self.chord._send_request(peer, {"action": "ADD_SORT_PARTITION", "job_id": job_id, "record": record_data})
                if response["status"] != "success":
                    print(f"Failed to add partition for job_id '{job_id}' to node {peer['node_id']}: {response.get('message', '')}")
                print(f"Sent partition for job_id '{job_id}' to node {peer['node_id']}")

    def collect_partition(self, job_id):
        sorted_peers = self.chord._sorted_peers()
        all_records = []
        for peer in sorted_peers:
            if self.chord._is_local(peer):
                partition_dir = os.path.join(self.chord.storage.base_dir, "sort_partitions")
                path = os.path.join(partition_dir, f"{job_id}.json")
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        records = json.load(f)
                else:
                    record_data = []
            else:
                response = self.chord._send_request(peer, {"action": "GET_SORT_PARTITION", "job_id": job_id})
                if response["status"] == "success":
                    record_data = response["records"]
                else:
                    print(f"Failed to get partition for job_id '{job_id}' from node {peer['node_id']}")
                    record_data = []
            for key, value in record_data:
                all_records.append((key, value))
        all_records.sort(key=lambda x: x[0])
        return all_records

    def sort_file(self, filename, output_filename):
        input = self.dfs.read(filename)
        if input["status"] != "success":
            return {"status": "error", "message": f"Failed to read file '{filename}': {input.get('message', '')}"}
        try:
            records = self.parse_record(input["content"])
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        job_id = hashlib.sha1(f"{filename}_{output_filename}".encode()).hexdigest()
        try:
            self._clear_partition(job_id)
            self.distribute_partition(job_id, records)
            sorted_records = self.collect_partition(job_id)
            sorted_data = "\n".join([f"{key},{value}" for key, value in sorted_records])
            result = self.dfs.write_sorted(output_filename, sorted_data)
            if result["status"] != "success":
                return {"status": "error", "message": f"Failed to write sorted output '{output_filename}': {result.get('message', '')}"}
            self._clear_partition(job_id)
            return result
        except Exception as e:
            return {"status": "error", "message": f"Sorting failed: {str(e)}"}