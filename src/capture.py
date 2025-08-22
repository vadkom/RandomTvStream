import os
import tempfile
from typing import Optional
from .util import which, run_quiet, log

FFMPEG = which("ffmpeg")
FFPROBE = which("ffprobe")

def probe_stream(url: str, seconds: int = 3) -> bool:
    """
    Quick probe to confirm there's a video stream.
    """
    cmd = [
        FFPROBE,
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "csv=p=0",
        url,
    ]
    rc = run_quiet(cmd, timeout=seconds + 3)
    return rc == 0

def capture_7s_reencode(url: str, out_path: str, seconds: int = 7) -> bool:
    """
    Capture ~7 seconds and normalize:
      - 720p, 30fps
      - H.264 (yuv420p), ~3Mbps
      - AAC 128k, 44.1kHz, stereo
      - Container: MPEG-TS
    """
    # Write to temp then atomically move
    fd, tmp_path = tempfile.mkstemp(prefix="clip_", suffix=".ts", dir=os.path.dirname(out_path))
    os.close(fd)
    os.unlink(tmp_path)  # ffmpeg will create it

    cmd = [
    FFMPEG,
    "-hide_banner",
    "-loglevel", "error",
    "-rw_timeout", "7000000",  # ~7s read timeout in microseconds
    "-i", url,
    "-t", str(seconds),
    "-vf", "scale=-2:720,fps=30",
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-b:v", "900k",        # average bitrate target
    "-maxrate", "1000k",   # cap
    "-bufsize", "2000k",   # buffer (controls burstiness)
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    "-b:a", "96k",         # smaller audio bitrate
    "-ar", "44100",
    "-ac", "2",
    "-f", "mpegts",
    tmp_path,
]

    rc = run_quiet(cmd, timeout=seconds + 20)
    if rc == 0 and os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 100_000:
        try:
            os.replace(tmp_path, out_path)
            return True
        except Exception as e:
            log(f"[ERR] Move failed: {e}")
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    else:
        # cleanup bad temp
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
    return False
