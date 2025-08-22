import os
import shutil
import subprocess
import time

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def which(bin_name: str) -> str:
    path = shutil.which(bin_name)
    if not path:
        raise RuntimeError(f"Required binary '{bin_name}' not found in PATH")
    return path

def run_quiet(cmd: list[str], timeout: int | None = None) -> int:
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
        return proc.returncode
    except subprocess.TimeoutExpired:
        return 124  # conventional timeout code

def log(msg: str):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), msg, flush=True)
