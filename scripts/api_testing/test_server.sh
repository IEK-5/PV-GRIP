#!/bin/bash
uploaded=$(./upload_raster.sh)
echo "uploaded tsv $uploaded"
rules_json="$(curl localhost:8081/api/osm/rules\?serve_type=path\&tsvfn_uploaded=$uploaded\&box='\[-100.0,-100.0,100.0,100.0\]')"
rules=$(echo $rules_json | jq -r .storage_fn)
echo "uploaded rules $rules"
if [ "$rules" = "null" ]; then
	echo "retry when rules file is done"
else
	rasters="$(curl localhost:8081/api/osm/route\?serve_type=path\&tsvfn_uploaded=$uploaded\&mesh_type=utm\&rulesfn_uploaded=$rules\&output_type=png\&step=1\&box='\[-100.0,-100.0,100.0,100.0\]')"
	json_path=$(echo $rasters | jq -r .storage_fn)
	curl localhost:8081/api/download\?path=$json_path\&serve_type="file" > rasters.json
fi
