curl -F data=@route_3.tsv  localhost:8081/api/upload | jq -r .storage_fn
