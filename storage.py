# local storage for metadata and page objects
import os
import json
import threading

class Storage:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.objects_dir = os.path.join(base_dir, 'objects')
        os.makedirs(self.objects_dir, exist_ok=True)
        self.lock = threading.Lock()

    def _convert_key_to_path(self, key):
        # convert a key to a file path in the objects directory
        return os.path.join(self.objects_dir, f"{key}.json")
    
    def store_object(self, key, obj):
        with self.lock:
            path = self._convert_key_to_path(key)
            with open(path, 'w') as f:
                json.dump(obj, f)

    def load_object(self, key):
        with self.lock:
            path = self._convert_key_to_path(key)
            if not os.path.exists(path):
                return None
            with open(path, 'r') as f:
                return json.load(f)
            
    def delete_object(self, key):
        with self.lock:
            path = self._convert_key_to_path(key)
            if os.path.exists(path):
                os.remove(path)
                return True
        return False

'''def main():
    # Example usage
    storage = Storage('data/node1')
    storage.store_object('example_key', {'foo': 'bar'})
    obj = storage.load_object('example_key')
    print(obj)  # Output: {'foo': 'bar'}
    deleted = storage.delete_object('example_key')
    print(deleted)  # Output: True

main()'''