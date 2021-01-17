#!/bin/bash

find sunrise_movie -type f | sort -V > files.txt
convert -delay xx @files.txt -loop 0 video.gif
ffmpeg -f gif -i video.gif video.mp4

rm files.txt video.gif
