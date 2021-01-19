#!/bin/bash

mkdir -p sunrise_movie

export url="localhost:8080"
export box="\[50.7741,6.0826,50.7766,6.0851\]"
export step=0.3
export data_re=".*_Las"

function do_stuff
{
    hour="$1"
    minute="$2"
    curl ${url}/api/shadow\?box="${box}"\&step="${step}"\&data_re="${data_re}"\&timestr="2021-06-21_${hour}:${minute}:00" \
         -o sunrise_movie/"${hour}":"${minute}".png
    echo "curl ${url}/api/shadow\?box=\"${box}\"\&step=\"${step}\"\&data_re=\"${data_re}\"\&timestr=\"2021-06-21_${hour}:${minute}:00\" \
         -o sunrise_movie/${hour}:${minute}.png"
}
export -f do_stuff

parallel -j8 do_stuff {1} {2} ::: $(seq 3 21) ::: $(seq 0 10 59)
