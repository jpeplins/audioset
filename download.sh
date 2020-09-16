#!/bin/bash

SAMPLE_RATE=16000
OUT_DIR="/data2/audioset-speech/files"

# fetch_clip(videoID, startTime, endTime)
fetch_clip() {
  echo "Fetching $1 ($2 to $3)..."
  outname="$1_$2"
  if [ -f "${outname}.wav.gz" ]; then
    echo "Already have it."
    return
  fi

  youtube-dl https://youtube.com/watch?v=$1 \
    --quiet --extract-audio --audio-format wav \
    --output "$outname.%(ext)s"
  if [ $? -eq 0 ]; then
    # If we don't pipe `yes`, ffmpeg seems to steal a
    # character from stdin. I have no idea why.
    yes | ffmpeg -loglevel quiet -i "./$outname.wav" -ac 1 -ar $SAMPLE_RATE \
      -ss "$2" -to "$3" "${OUT_DIR}/${outname}.wav"
    rm "./$outname.wav"
  else
    echo "Issue downloading file ${outname}.wav"
  fi
}

grep -E '^[^#]' | while read line
do
  fetch_clip $(echo "$line" | sed -E 's/, / /g')
done
