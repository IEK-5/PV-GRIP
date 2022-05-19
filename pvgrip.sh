#!/bin/bash

cd $(git rev-parse --show-toplevel)


function set_defaults {
    network_interfaces=$(python3 scripts/get_config.py \
                                 server interfaces)
    docker_maxmemory=$(echo "scale=2; $(grep MemTotal /proc/meminfo | awk '{print $2}')/1024/1024*0.9" \
                           | bc | awk '{printf "%.2f", $0}')"g"
    what="webserver"
    webserver_hostport=$(python3 scripts/get_config.py \
                                 webserver hostport)
    flower_hostport="55555"
    ifrestart="yes"
    mnt_data="$(pwd)/data"
    mnt_configs="$(pwd)/configs"
    mnt_docs="$(pwd)/docs"
    ifmntcode="no"
    name_prefix="pvgrip"
    registry=$(python3 scripts/get_config.py \
                       server docker_registry)
    image_tag=$(python3 scripts/get_config.py \
                        server image_tag)
    next_tag=$(./scripts/get_version.sh)
    prunekeep=2
    ifdry="no"
}


function print_help {
    echo "Usage: $0 [--argument=value ...]"
    echo
    echo "Start webserver/worker/broker or build the image"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  -d,--dry          just echo command, do not execute"
    echo
    echo "  --what            action: start \"webserver\", \"worker\", \"worker_requests\","
    echo "                    \"broker\", \"flower\""
    echo "                    \"build\", \"tag\", \"push\", \"pull\", \"prune\" image."
    echo "                    Default: \"${what}\""
    echo
    echo "  --mnt-data        mount point of the data directory"
    echo "                    Default: \"${mnt_data}\""
    echo
    echo "  --mnt-configs     mount point of the configs directory"
    echo "                    Default: \"${mnt_configs}\""
    echo
    echo "  --registry        repository location. Use empty for locally built images"
    echo "                    Default: \"${registry}\""
    echo
    echo "  --image-tag       image tag (version) to use."
    echo "                    Default: \"${image_tag}\""
    echo
    echo "  --name-prefix     prefix of the docker containers' names"
    echo "                    Default: \"${name_prefix}\""
    echo
    echo "  --maxmemory       hard limit on memory"
    echo "                    Default: ${docker_maxmemory}"
    echo
    echo "  --network         network interface where to bind ports"
    echo "                    Empty for all. A comma separeted list."
    echo "                    Use - for global bind."
    echo "                    Default: \"${network_interfaces}\""
    echo
    echo "  --ifrestart       enable restart on docker restart."
    echo "                    Default: \"${ifrestart}\""
    echo
    echo "  --ifmntcode       if mount source code."
    echo "                    If yes, data and configs are used from the git root."
    echo "                    Default: \"${ifmntcode}\""
    echo
    echo "  --next-tag        the version to use in the next tag."
    echo "                    Used only with --what=tag and --what=push commands."
    echo "                    Default: \"${next_tag}\""
    echo
    echo "  --prunekeep       number of images to keep at pruning."
    echo "                    Default: \"${prunekeep}\""
    echo
    echo "  --webserver-hostport"
    echo "                    port to bind webserver to."
    echo "                    Leave empty to bind to --network."
    echo "                    Default: \"${webserver_hostport}\""
    echo
    echo "  --flower-hostport"
    echo "                    port to bind flower to."
    echo "                    Leave empty to bind to --network."
    echo "                    Default: \"${webserver_hostport}\""
    echo
}


function parse_args {
    for i in "$@"
    do
        case "${i}" in
	        --maxmemory=*)
		        docker_maxmemory="${i#*=}"
		        shift
		        ;;
            --network=*)
                network_interfaces="${i#*=}"
                shift
                ;;
            --what=*)
                what="${i#*=}"
                shift
                ;;
            --ifrestart=*)
                ifrestart="${i#*=}"
                shift
                ;;
            --mnt-data=*)
                mnt_data="${i#*=}"
                shift
                ;;
            --mnt-configs=*)
                mnt_configs="${i#*=}"
                shift
                ;;
            --ifmntcode=*)
                ifmntcode="${i#*=}"
                shift
                ;;
            --name-prefix=*)
                name_prefix="${i#*=}"
                shift
                ;;
            --registry=*)
                registry="${i#*=}"
                shift
                ;;
            --image-tag=*)
                image_tag="${i#*=}"
                shift
                ;;
            --next-tag=*)
                next_tag="${i#*=}"
                shift
                ;;
            --prunekeep=*)
                prunekeep="${i#*=}"
                shift
                ;;
            --webserver-hostport=*)
                webserver_hostport="${i#*=}"
                shift
                ;;
            -d|--dry)
                ifdry="yes"
                ;;
            -h|--help)
                print_help
                exit
                ;;
            *)
                echo "unknown argument!"
                exit
                ;;
        esac
    done
}


function get_ip {
    interface="$1"
    if [ -z "${interface}" ]
    then
        echo ""
        return
    fi

    bind_ip=$(ip -f inet addr show "${interface}" | awk '/inet/ {print $2}' | cut -d/ -f1)
    echo "${bind_ip}"
}


function get_restart {
    if [ "yes" = "${ifrestart}" ]
    then
        echo " --restart always "
        return
    fi

    echo ""
    return
}


function prune_byname {
    expr="$1"
    ids=$(docker container ls -a | \
              grep "${expr}\$" | \
              awk '{print $1}' | xargs)
    if [ ! -z "${ids}" ]
    then
        if [ "yes" = "${ifdry}" ]
        then
            echo docker container rm "${ids}"
            return 0
        fi

        docker container rm "${ids}"
        [ "$?" -ne 0 ] && return 1
    fi
    return 0
}


function start_preamble {
    res="docker run -d -t -i"
    res+=" --hostname $(hostname)-${what}"
    res+=" --memory ${docker_maxmemory}"
    res+=" --name ${name_prefix}-${what}"
    res+=" $(get_restart)"
    echo "echo ${res}"
}


function mount_volumes {
    if [ "yes" = "${ifmntcode}" ]
    then
        echo -v "$(pwd):/code" $(scripts/get_mounts.py)
        return
    fi

    echo -v "${mnt_data}:/code/data" -v "${mnt_configs}:/code/configs" $(scripts/get_mounts.py)
}


function get_binding {
    ports="$1"
    res=""
    for interface in $(echo "${network_interfaces}" | tr "," " ")
    do
        if [ "-" = "${interface}" ]
        then
            res+=" -p ${ports}"
            continue
        fi
        res+=" -p $(get_ip ${interface}):${ports}"
    done

    echo "${res}"
}


function start_webserver {
    bind="$(get_binding 8080:8080)"
    if [ ! -z "${webserver_hostport}" ]
    then
        bind+=" -p ${webserver_hostport}:8080"
    fi

    $(start_preamble) \
        $(mount_volumes) \
	" --user $(id -u):$(id -g)" \
        "${bind}" \
        "${registry}${name_prefix}:${image_tag}" \
        ./scripts/start.sh --what="${what}"
}


function start_worker {
    $(start_preamble) \
        $(mount_volumes) \
	" --user $(id -u):$(id -g)" \
        "${registry}${name_prefix}:${image_tag}" \
        ./scripts/start.sh --what="${what}"
}


function start_broker {
    $(start_preamble) \
        "$(get_binding 6379:6379)" \
        redis
}


function start_flower {
    bind="$(get_binding 5555:5555)"
    bind+=" -p ${flower_hostport}:5555"

    $(start_preamble) \
        $(mount_volumes) \
        -v "${mnt_docs}:/data" \
	" --user $(id -u):$(id -g)" \
        "${bind}" \
        "${registry}${name_prefix}:${image_tag}" \
        ./scripts/start.sh --what="${what}"
}


function prune_old_local {
    images=$(docker images | \
                 grep "${registry}${name_prefix}" | \
                 grep '<none>' | \
                 awk '{print $3}' | \
                 xargs)
    images+=" "$(docker images | \
                 grep "${registry}${name_prefix}" | \
                 grep -v '<none>' | \
                 grep -v latest | \
                 sort -V | head -n -${prunekeep} | \
                 awk '{print $3}' | \
                 xargs)

    if [ "${images}" != " " ]
    then
        echo docker rmi "${images}"
        return 0
    fi

    return 0
}

set_defaults
parse_args $@
[ ! -z "${registry}" ] && registry="${registry}/"

case "${what}" in
    webserver)
        prune_byname "${name_prefix}-${what}" || exit 1
        docommand=$(start_webserver)
        ;;
    worker)
        prune_byname "${name_prefix}-${what}" || exit 1
        docommand=$(start_worker)
        ;;
    worker_requests)
        prune_byname "${name_prefix}-${what}" || exit 1
        docommand=$(start_worker)
        ;;
    broker)
        prune_byname "${name_prefix}-${what}" || exit 1
        docommand=$(start_broker)
        ;;
    flower)
        prune_byname "${name_prefix}-${what}" || exit 1
        docommand=$(start_flower)
        ;;
    build)
        docommand=$(echo "git submodule update --init --recursive; docker build -t ${name_prefix} .")
        ;;
    tag)
        docommand=$(echo docker tag "${name_prefix}" "${registry}${name_prefix}:${next_tag}")
        ;;
    push)
        docommand=$(echo docker push "${registry}${name_prefix}:${next_tag}")
        ;;
    pull)
        docommand=$(echo docker pull "${registry}${name_prefix}:${image_tag}")
        ;;
    prune)
        docommand=$(prune_old_local)
        ;;
    *)
        echo "unknown value of --what!"
        exit
        ;;
esac


if [ "yes" = "${ifdry}" ]
then
    echo ${docommand}
    exit 0
fi

eval ${docommand}
