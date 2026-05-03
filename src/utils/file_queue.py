import json
import os
from threading import Lock

FILE_PATH = "data/queue.json"
file_lock = Lock()

def load_data():
    if not os.path.exists(FILE_PATH):
        return {}
    with open(FILE_PATH, "r") as f:
        try:
            return json.load(f)
        except:
            return {}

def save_data(data):
    with open(FILE_PATH, "w") as f:
        json.dump(data, f)

def enqueue(key, message):
    with file_lock:
        data = load_data()
        if key not in data:
            data[key] = []
        data[key].append(message)
        save_data(data)
        print(f"[QUEUE] enqueue key={key}, message={message}")
        return {"status": "enqueued"}

def dequeue(key):
    with file_lock:
        data = load_data()
        if key not in data or len(data[key]) == 0:
            return {"message": None}

        message = data[key].pop(0)
        save_data(data)
        print(f"[QUEUE] dequeue key={key}, message={message}")
        return {"message": message}