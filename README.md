r/Place player
==============

This is supposed to be an interactive player for r/Place, whenever data is available. 
For now it produces a video stream, and requires ffmpeg to render to a video file.

For now this is more like a script than an actual program so you need to mess around in main.rs to parse the CSV or just load pre-parsed data.
Download the dataset from one of these places:

Reddit archive: https://storage.googleapis.com/justin_bassett/place_tiles

Then create a folder called data on this repo, and place the file there.

Run `ffmpeg_render.sh` to render. You need ffmpeg installed and on PATH. Check https://www.youtube.com/watch?v=FWT4d8bwOnU for a demo.
