#!/bin/bash

cd $(git rev-parse --show-toplevel)

while IFS= read -r line
do
    host=$(echo "$line" | awk '{print $1;}')
    args=$(echo "$line" | awk '{for (i=2;i<NF;++i) printf $i " "; print $NF}')

    ssh $host /bin/bash <<EOF
    cd pvgrip
    ./restart.sh ${args}
EOF
done < "./configs/hosts"
