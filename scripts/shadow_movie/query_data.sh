#!/bin/bash

mkdir -p sunrise_movie

export url="localhost:8080"
export box="\[50.6046,6.3794,50.6098,6.3977\]"
export step=1
export datare=".*_max"

function do_stuff
{
    hour="$1"
    minute="$2"
    curl ${url}/api/v1/shadow\?box="${box}"\&step="${step}"\&datare="${datare}"\&timestr="2021-06-21_${hour}:${minute}:00" \
         -o sunrise_movie/"${hour}":"${minute}".png
}
export -f do_stuff

parallel -j64 do_stuff {1} {2} ::: $(seq 3 21) ::: $(seq 0 5 59)
