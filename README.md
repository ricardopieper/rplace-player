r/Place player
==============

This is supposed to be an interactive player for r/Place, whenever data is available. 
For now it produces a video stream, and requires ffmpeg to render to a video file.

For now this is more like a script than an actual program so you need to mess around in main.rs to parse the CSV or just load pre-parsed data.
Download the dataset from one of these places:

https://storage.googleapis.com/justin_bassett/place_tiles
https://ipfs.io/ipfs/Qmbqvdaq5yLaz1fPCN2wYywotieAnhJc2rNCenuvnTQ8Ei?filename=rplace_2017_tile_placements.csv

Then create a folder called data on this repo, and place the file there.

Run `ffmpeg_render.sh` to render. You need ffmpeg installed and on PATH. Check https://www.youtube.com/watch?v=FWT4d8bwOnU for a demo.