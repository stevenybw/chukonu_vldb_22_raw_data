import pickle
import tempfile

def dump(o, path = None):
    if not path:
        path = tempfile.mktemp()
    with open(path, "wb") as f:
        pickle.dump(o, f)
    print(f"Dumped into {path}")

def load(path):
    with open(path, "rb") as f:
        return pickle.load(f)