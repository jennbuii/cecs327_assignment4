# DFS Layer on top of ChordNode
# knows what a file is, what metadata looks like, how to split files into pages, and how to store metadata and pages using the ChordNode's storage layer
import hashlib
from chord_node import ChordNode
import tempfile
import os

class DFSAPI:
    def __init__(self, chord, paxos=None, page_size=1024):
        self.chord = chord
        self.paxos = paxos
        self.page_size = page_size

    #helper functions
    def _hash(self, key):
        return hashlib.sha1(key.encode()).hexdigest()
    
    def _metadata_key(self, filename):
        return self._hash(f"metadata:{filename}")
    
    def _page_key(self, filename, page_no):
        return self._hash(f"page:{filename}:{page_no}")
    
    def _index_key(self):
        return self._hash("metadata:index")
    
    def _get_index(self):
        key = self._index_key()
        obj = self.chord.get(key)
        if obj["status"] != "success":
            return {"kind": "index", "filenames": []}
        return obj["object"]
    
    def _save_index(self, index):
        key = self._index_key()
        self.chord.put(key, index)

    def _add_to_index(self, filename):
        index = self._get_index()
        if filename not in index["filenames"]:
            index["filenames"].append(filename)
            index["filenames"].sort()
            self._save_index(index)
    
    def _remove_from_index(self, filename):
        index = self._get_index()
        if filename in index["filenames"]:
            index["filenames"].remove(filename)
            self._save_index(index)

    def _split_into_pages(self, data):
        pages = []
        for i in range(0, len(data), self.page_size):
            pages.append(data[i:i+self.page_size])
        return pages

    #paxos helper functions
    def apply_op(self, op):
        op_type = op["op"]
        if op_type == "touch":
            return self._apply_touch(op["filename"])
        elif op_type == "append":
            return self._apply_append_content(op["filename"], op["content"])
        elif op_type == "delete_file":
            return self._apply_delete_file(op["filename"])
        else:
            return {"status": "error", "message": f"Unknown operation type '{op_type}'."}

    #paxos methods
    def touch(self, filename):
        if self.paxos is None:
            return self._apply_touch(filename)
        op = {"op": "touch", "filename": filename}
        return self.paxos.propose(op)
    
    def append(self, filename, local_path):
        try:
            with open(local_path, 'r') as f:
                data = f.read()
        except Exception as e:
            return {"status": "error", "message": f"Failed to read local file: {str(e)}"}
        chunks = self._split_into_pages(data)
        
        if self.paxos is None:
            return self._apply_append(filename, local_path)
        op = {"op": "append", "filename": filename, "content": data}
        return self.paxos.propose(op)
    
    def delete_file(self, filename):
        if self.paxos is None:
            return self._apply_delete_file(filename)
        op = {"op": "delete_file", "filename": filename}
        return self.paxos.propose(op)

    #part a methods
    def _apply_touch(self, filename):
        # create empty file metadata and add to index
        key = self._metadata_key(filename)
        file = self.chord.get(key)
        if file["status"] == "success":
            return {"status": "error", "message": f"File '{filename}' already exists."}
        metadata = {
            "kind": "metadata",
            "filename": filename,
            "size_bytes": 0,
            "num_pages": 0,
            "pages": [],
            "version": 1
        }
        self.chord.put(key, metadata)
        self._add_to_index(filename)
        return {"status": "success", "message": f"File '{filename}' created."}
    
    def stat(self, filename):
        # return file metadata
        key = self._metadata_key(filename)
        result = self.chord.get(key)
        if result["status"] != "success":
            return {"status": "error", "message": f"File '{filename}' not found."}
        return {"status": "success", "metadata": result["object"]}
    
    def ls(self):
        # return list of all files
        index = self._get_index()
        return {"status": "success", "filenames": index["filenames"]}
    
    def _apply_append(self, filename, local_path):
        # append data from local file to DFS file
        key = self._metadata_key(filename)
        result = self.chord.get(key)
        if  result["status"] != "success":
            return {"status": "error", "message": f"File '{filename}' not found."}
        
        metadata = result["object"]
        try:
            with open(local_path, 'r') as f:
                data = f.read()
        except Exception as e:
            return {"status": "error", "message": f"Failed to read local file: {str(e)}"}
        
        chunks = self._split_into_pages(data)
        start_page_no = metadata["num_pages"]
        for i, chunk in enumerate(chunks):
            page_no = start_page_no + i
            page_key = self._page_key(filename, page_no)
            self.chord.put(page_key, {"kind": "page", "filename": filename, "page_no": page_no, "data": chunk})
            page_descriptor = {"page_no": page_no, "page_key": page_key, "size_bytes": len(chunk)}
            metadata["pages"].append(page_descriptor)

        metadata["num_pages"] += len(chunks)
        metadata["size_bytes"] += len(data)
        metadata["version"] += 1
        self.chord.put(key, metadata)
        return {"status": "success", "message": f"Appended {len(data)} bytes to '{filename}'.", "num_pages": len(chunks)}
    
    def _apply_append_content(self, filename, data):
        key = self._metadata_key(filename)
        result = self.chord.get(key)
        if  result["status"] != "success":
            return {"status": "error", "message": f"File '{filename}' not found."}
        metadata = result["object"]
        chunks = self._split_into_pages(data)
        start_page_no = metadata["num_pages"]
        for i, chunk in enumerate(chunks):
            page_no = start_page_no + i
            page_key = self._page_key(filename, page_no)
            self.chord.put(page_key, {"kind": "page", "filename": filename, "page_no": page_no, "data": chunk})
            metadata["pages"].append({"page_no": page_no, "page_key": page_key, "size_bytes": len(chunk)})
        metadata["num_pages"] += len(chunks)
        metadata["size_bytes"] += len(data)
        metadata["version"] += 1
        self.chord.put(key, metadata)
        return {"status": "success", "message": f"Appended {len(data)} bytes to '{filename}'.", "num_pages": len(chunks)}

    def read(self, filename):
        # read all pages for a file
        key = self._metadata_key(filename)
        result = self.chord.get(key)
        if result["status"] != "success":
            return {"status": "error", "message": f"File '{filename}' not found."}
        data = []

        for descriptor in result["object"]["pages"]:
            page_key = descriptor["page_key"]
            page = self.chord.get(page_key)
            if page["status"] != "success":
                return {"status": "error", "message": f"Page {descriptor['page_no']} not found for file '{filename}'."}
            data.append(page["object"]["data"])
        return {"status": "success", "content": "".join(data)}
    
    def _apply_delete_file(self, filename):
        # delete metadata and all pages for a file
        key = self._metadata_key(filename)
        result = self.chord.get(key)
        if result["status"] != "success":
            return {"status": "error", "message": f"File '{filename}' not found."}

        for descriptor in result["object"]["pages"]:
            page_key = descriptor["page_key"]
            self.chord.delete(page_key)
        self.chord.delete(key)
        self._remove_from_index(filename)
        return {"status": "success", "message": f"File '{filename}' deleted."}
    
    def head(self, filename, n):
        head = self.read(filename)
        if head["status"] != "success":
            return head
        lines = head["content"].splitlines()
        return {"status": "success", "content": "\n".join(lines[:n])}
    
    def tail(self, filename, n):
        tail = self.read(filename)
        if tail["status"] != "success":
            return tail
        lines = tail["content"].splitlines()
        return {"status": "success", "content": "\n".join(lines[-n:])}