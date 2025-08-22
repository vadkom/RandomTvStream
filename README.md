# Random YouTube Streamer (24/7)

Randomly samples live M3U8 streams, captures 7-second normalized clips, buffers them, and pushes a continuous RTMP feed to YouTube.

## Requirements
- Linux recommended (named pipe / FIFO)
- Python 3.10+
- FFmpeg (`ffmpeg`, `ffprobe`) in PATH
- `pip install -r requirements.txt`

## Configure
1. Put your YouTube RTMP URL in `config/youtube_rtmp.txt`:

2. Optionally change the source playlist URL in `config/playlist_url.txt`.

## Run
```bash
cd src
python -m streamer
