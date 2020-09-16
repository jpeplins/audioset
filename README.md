# Audioset Scraper

This is a tool for downloading raw audio from [AudioSet](https://research.google.com/audioset/), a dataset of labeled audio files. This tool only works in Unix-like environments, sorry.

This version is modified by [Jacob Peplinski](https://github.com/jpeplins) to support
* Multiprocessing!
* Label Filtering!
* Commercial Flight Simulation! (TBD)

The original can be found [here](https://github.com/unixpickle/audioset).
 
# Dependencies

 * bash
 * ffmpeg
 * youtube-dl
 * gzip

# Usage
After adjusting the `OUT_DIR` variable in `download.sh` to your liking, open a terminal and do the following:

```
virtualenv venv
source venv/bin/activate
pip3 install -r requirements.txt
python3 run_scraper.py
```
