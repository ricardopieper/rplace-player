cargo run --release | ffmpeg.exe -y  -f image2pipe -framerate 60 -i -  -vf format=yuv420p -video_size 1000x1000  out.mp4