import os
import random
import requests
from typing import List
from .util import log

def load_blocklist(path: str = "blocklist.txt") -> List[str]:
    """
    Load blocklist from a file. Ignores empty lines and comments (#).
    """
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [
            line.strip().lower()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]

def fetch_m3u_urls(m3u_url: str, timeout: int = 10, blocklist_path: str = "blocklist.txt") -> List[str]:
    try:
        r = requests.get(m3u_url, timeout=timeout)
        r.raise_for_status()

        blocklist = load_blocklist(blocklist_path)
        raw_urls = [line.strip() for line in r.text.splitlines() if line and not line.startswith("#")]

        if blocklist:
            raw_count = len(raw_urls)
            urls = [u for u in raw_urls if all(b not in u.lower() for b in blocklist)]
            filtered_out = raw_count - len(urls)
            if filtered_out:
                log(f"Filtered out {filtered_out} blocked URLs")
        else:
            urls = raw_urls

        random.shuffle(urls)
        log(f"Playlist loaded: {len(urls)} URLs")
        return urls
    except Exception as e:
        log(f"[WARN] Failed to fetch playlist: {e}")
        return []
    
def rotate_candidates(cands: List[str]):
    """Generator that yields random choices from the list forever."""
    pool = list(cands)
    while True:
        if not pool:
            yield None
        else:
            yield random.choice(pool)
