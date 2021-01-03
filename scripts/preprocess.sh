#!/bin/bash

# This converts all data to a common format, split file on smaller chunks

cd $(git rev-parse --show-toplevel)

export src_dir="data/current"
export chunk_size=3000

function convert_to_wgs84 {
    file="$1"

    if gdalinfo "${file}" | grep -q -F 'ELLIPSOID["WGS 84"'
    then
        return
    fi

    "Converting to WGS 84 coordinates"
    gdalwarp "${file}" "${file}_converted.tif" -t_srs "+proj=longlat +ellps=WGS84"
    mv "${file}_converted.tif" "${file}"
}
export -f convert_to_wgs84

function split_tiles {
    file="$1"

    xsize=$(gdalinfo "${file}" | grep -F "Size is" | sed 's/Size is \(.*\),\(.*\)/\1/')
    ysize=$(gdalinfo "${file}" | grep -F "Size is" | sed 's/Size is \(.*\),\(.*\)/\2/')

    xtiles=$(echo "1 + ${xsize}/${chunk_size}" | bc)
    ytiles=$(echo "1 + ${ysize}/${chunk_size}" | bc)

    if [ "${xtiles}" -gt 1 ] || [ "${ytiles}" -gt 1 ]
    then
        scripts/create-tiles.sh "${file}" "${xtiles}" "${ytiles}" && rm "${file}"
    fi
}
export -f split_tiles

function preprocess {
    file="$1"

    echo -e "\nProcessing: ${file}"

    # ignore file if not supported by gdal
    if ! gdalinfo "${file}" &> /dev/null
    then
        echo "File is not supported by GDAL. Ignoring..."
        return
    fi

    convert_to_wgs84 "${file}"
    split_tiles "${file}"
}
export -f preprocess

cp -rl "${src_dir}" "${src_dir}.bak"
find "${src_dir}" -type f -exec bash -c 'preprocess "$0"' {} \;
