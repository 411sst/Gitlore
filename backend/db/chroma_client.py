import chromadb
import os

DB_PATH = "./data/chroma"

_client = None

def init_chroma() -> chromadb.PersistentClient:
    global _client
    os.makedirs(DB_PATH, exist_ok=True)
    _client = chromadb.PersistentClient(path=DB_PATH)
    return _client

def get_collection(name: str) -> chromadb.Collection:
    if _client is None:
        raise RuntimeError("Chroma client not initialized. Call init_chroma() first.")
    return _client.get_or_create_collection(name)
