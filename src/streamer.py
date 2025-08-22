#!/usr/bin/env python3
import os
import threading
import queue
import time
import stat
import random
import datetime
from pathlib import Path
from typing import Optional
import uuid

from .util import ensure_dir, which, log, run_quiet
from .playlist import fetch_m3u_urls
from .capture import probe_stream, capture_7s_reencode

# ---------- CONFIG ----------
ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config"
BUFFER_DIR = ROOT / "buffer"
FIFO_PATH = BUFFER_DIR / "mux.ts"       # Named pipe feeding ffmpeg
RTMP_FILE = CONFIG / "youtube_rtmp.txt"
PLAYLIST_FILE = CONFIG / "playlist_url.txt"

MAX_QUEUE = 14
MIN_QUEUE = 7
WORKERS = 4
STALE_SEC = 30           # Max age for queued clips
BUFFER_CLEANUP_SEC = 120 # Remove orphaned clips older than this
CLEANUP_INTERVAL = 10    # Queue cleanup cadence
# ---------------------------

FFMPEG = which("ffmpeg")

# ---------- CLIP QUEUE ----------
class ClipQueue:
    def __init__(self, max_items: int):
        self.q = queue.Queue(maxsize=max_items)
        self.lock = threading.Lock()
        self.last_good: Optional[Path] = None
        self.timestamps = {}
        self._stop_event = threading.Event()
        threading.Thread(target=self._cleanup_loop, daemon=True).start()

    def put(self, path: Path, timeout: Optional[int] = None):
        self.q.put(path, timeout=timeout)
        self.timestamps[path] = time.time()

    def get(self, timeout: Optional[int] = None) -> Optional[Path]:
        try:
            while True:
                p = self.q.get(timeout=timeout)
                age = time.time() - self.timestamps.pop(p, 0)
                if age > STALE_SEC:
                    log(f"[STALE] Discarding {p.name}, age={age:.1f}s")
                    try: p.unlink(missing_ok=True)
                    except Exception: pass
                    continue
                with self.lock:
                    self.last_good = p
                return p
        except queue.Empty:
            return None

    def size(self) -> int:
        return self.q.qsize()

    def last(self) -> Optional[Path]:
        with self.lock:
            return self.last_good

    def stop(self):
        self._stop_event.set()

    def _cleanup_loop(self):
        while not self._stop_event.is_set():
            now = time.time()
            removed = 0
            with self.q.mutex:
                fresh_items = []
                while self.q.queue:
                    p = self.q.queue.popleft()
                    age = now - self.timestamps.get(p, 0)
                    if age > STALE_SEC:
                        removed += 1
                        self.timestamps.pop(p, None)
                        try: p.unlink(missing_ok=True)
                        except Exception: pass
                    else:
                        fresh_items.append(p)
                for item in fresh_items:
                    self.q.queue.append(item)
            if removed:
                log(f"[QUEUE CLEANUP] Removed {removed} stale clips")
            time.sleep(CLEANUP_INTERVAL)

# ---------- FIFO ----------
def make_fifo():
    ensure_dir(BUFFER_DIR)
    if FIFO_PATH.exists():
        try:
            if stat.S_ISFIFO(os.stat(FIFO_PATH).st_mode):
                return
        except Exception:
            pass
        try: FIFO_PATH.unlink(missing_ok=True)
        except TypeError:
            try: FIFO_PATH.unlink()
            except FileNotFoundError: pass
    os.mkfifo(FIFO_PATH)

# ---------- PUSHER ----------
def reader_thread(rtmp_url: str, cq: ClipQueue):
    push_cmd = [
        FFMPEG,
        "-re",
        "-i", str(FIFO_PATH),
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-b:v", "900k",
        "-maxrate", "1000k",
        "-bufsize", "2000k",
        "-pix_fmt", "yuv420p",
        "-g", "60",
        "-c:a", "aac",
        "-b:a", "96k",
        "-ar", "44100",
        "-ac", "2",
        "-f", "flv",
        rtmp_url,
    ]

    def pusher():
        log("[PUSH] Starting ffmpeg → YouTube")
        run_quiet(push_cmd)

    while True:
        make_fifo()
        threading.Thread(target=pusher, daemon=True).start()

        try:
            with open(FIFO_PATH, "wb", buffering=0) as fifo:
                log("[MUX] FIFO opened for writing")
                while True:
                    clip_path = cq.get(timeout=5)
                    if clip_path is None:
                        last = cq.last()
                        if last and last.exists():
                            log("[MUX] Queue empty, repeating last clip")
                            with open(last, "rb") as f:
                                for chunk in iter(lambda: f.read(1024*1024), b""):
                                    fifo.write(chunk)
                            continue
                        time.sleep(1)
                        continue

                    if clip_path.exists():
                        with open(clip_path, "rb") as f:
                            for chunk in iter(lambda: f.read(1024*1024), b""):
                                fifo.write(chunk)
                        try: clip_path.unlink()
                        except Exception as e: log(f"[WARN] Could not delete {clip_path.name}: {e}")
                    else:
                        log(f"[SKIP] Missing clip: {clip_path}")
        except (BrokenPipeError, OSError):
            log("[MUX] Broken pipe, restarting pusher...")
            time.sleep(1)
            continue

# ---------- WORKER ----------
def worker_thread(name: str, holder: dict, cq: ClipQueue, stop_evt: threading.Event, pause_evt: threading.Event):
    duration_cycle = 0
    while not stop_evt.is_set():
        if pause_evt.is_set():
            time.sleep(2)
            continue
        urls = holder.get("urls", [])
        if not urls:
            time.sleep(2)
            continue

        url = random.choice(urls)
        if not probe_stream(url, seconds=3):
            continue

        clip_len = [5,7,11][duration_cycle]
        duration_cycle = (duration_cycle +1)%3
        clip_path = BUFFER_DIR / f"clip_{uuid.uuid4().hex[:8]}.ts"

        ok = capture_7s_reencode(url, str(clip_path), seconds=clip_len)
        if ok and clip_path.exists():
            try: cq.put(clip_path, timeout=5)
            except queue.Full: clip_path.unlink(missing_ok=True)
        else:
            clip_path.unlink(missing_ok=True)

# ---------- PLAYLIST RELOAD ----------
def reload_playlist_at_midnight(holder: dict, stop_evt: threading.Event):
    while not stop_evt.is_set():
        now = datetime.datetime.now()
        tomorrow = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time.sleep((tomorrow - now).total_seconds())
        if stop_evt.is_set(): break
        try:
            urls = fetch_m3u_urls((PLAYLIST_FILE).read_text().strip())
            if urls: holder["urls"] = urls
        except Exception as e:
            log(f"[PLAYLIST] Reload error: {e}")

# ---------- BUFFER CLEANUP ----------
def cleanup_buffer(cq: ClipQueue, stop_evt: threading.Event, threshold: int = BUFFER_CLEANUP_SEC):
    while not stop_evt.is_set():
        queued = set()
        with cq.q.mutex: queued.update(cq.q.queue)
        last = cq.last()
        if last: queued.add(last)
        removed = 0
        for clip in BUFFER_DIR.glob("*.ts"):
            if clip not in queued and clip != FIFO_PATH:
                try: age = time.time() - clip.stat().st_mtime
                except FileNotFoundError: continue
                if age > threshold:
                    try: clip.unlink(); removed +=1
                    except Exception: pass
        if removed: log(f"[BUFFER CLEANUP] Removed {removed} orphaned clips")
        time.sleep(60)

# ---------- BUFFER MONITOR ----------
def buffer_monitor(cq: ClipQueue, pause_evt: threading.Event, stop_evt: threading.Event):
    while not stop_evt.is_set():
        buf_size = cq.size()
        pause_evt.set() if buf_size >= MAX_QUEUE else pause_evt.clear() if buf_size <= MIN_QUEUE else None
        time.sleep(2)

# ---------- MAIN ----------
def main():
    ensure_dir(BUFFER_DIR)
    rtmp_url = (RTMP_FILE).read_text().strip()
    if not rtmp_url.startswith("rtmp://"): raise RuntimeError("youtube_rtmp.txt must contain a valid RTMP URL")

    holder = {"urls": fetch_m3u_urls((PLAYLIST_FILE).read_text().strip())}
    cq = ClipQueue(max_items=MAX_QUEUE)
    stop_evt, pause_evt = threading.Event(), threading.Event()

    threading.Thread(target=reload_playlist_at_midnight, args=(holder, stop_evt), daemon=True).start()
    threading.Thread(target=buffer_monitor, args=(cq, pause_evt, stop_evt), daemon=True).start()
    threading.Thread(target=cleanup_buffer, args=(cq, stop_evt), daemon=True).start()

    for i in range(WORKERS):
        threading.Thread(target=worker_thread, args=(f"W{i+1}", holder, cq, stop_evt, pause_evt), daemon=True).start()

    log(f"[BOOT] Warming buffer until {MIN_QUEUE} clips ready...")
    while cq.size() < MIN_QUEUE: time.sleep(1)
    log("[BOOT] Buffer ready — starting pusher")
    reader_thread(rtmp_url, cq)

if __name__ == "__main__":
    main()
