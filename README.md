# PV-GRIP (PV-Geographic Raster Image Processor)

[![Find a bird!](http://img.youtube.com/vi/GGpmm5at-a8/0.jpg)](http://www.youtube.com/watch?v=GGpmm5at-a8)

A scalable server that provides access to geospatial data. This is a
fork of [https://open-elevation.com](https://open-elevation.com)
updated and extended by few things I need in my project.

## Installation

To run the PV-GRIP each node should have a docker installed, with user
privileges to run docker images (optionally privileges to set up
network interfaces).

To install get all necessary code for the PV-GRIP say
```
git clone git@github.com:esovetkin/open-elevation.git
cd open-elevation
make submodule
```

## Running the server

The PV-GRIP consists of several components: storage ([cassandra
storage](https://cassandra.apache.org/)), message broker
([redis](https://redis.io/)), processing nodes
([celery](https://docs.celeryproject.org/en/stable/)) and the
webserver ([bottle](https://bottlepy.org/docs/dev/)).

The webserver listens to the user requests, builds the processing
pipelines and serves the collected results back to user. The
communication between the webserver and the processing nodes are
handled by the message broker. The messages are strings (required for
accessing the data in the distributed storage) or dictionaries
(specifying a required job parameters).

The message broker handles distributing processing pipeline tasks
among processing nodes and collecting results. The message broker runs
on the same node as the webserver. The broker should be accessible to
every processing node.

The distributed tasks queue runs processing jobs from the pipeline
built by the webserver. Multiple processing nodes can be run.

The storage handles data storage, webserver cache and spatial
indexing. The storage is run on multiple nodes providing robustness
against node failures.

Server can be run on multiple nodes. Each node can be a storage unit,
a broker, a processing worker or a webserver (or all of that
together).

Below are provided steps assisting setting up the network, storage,
workers and the webserver nodes.  All commands below are assumed to be
run in the root of the git repository.

### Setting up network

It is recommended to set up a [Wireguard](https://www.wireguard.com/)
network for communications between storage/worker nodes.

Below it is assumed that each node has a network interface `pvgrip0`
set up, such that each node can reach another one within this
network. Below we assume that the `node1` has an ip address
`10.0.0.1`, whereas the `node2` has an ip address `10.0.0.2`, etc.

### Setting up cassandra storage

To start the first cassandra node on the `node1` say
```
cd open_elevation/cassandra_io
./scripts/start_cassandra.sh --broadcast=10.0.0.1
```

Several arguments can be specified, e.g. mount point `--mnt` where the
actual data resides, used `--max_heap_size` for the maximum RAM being
allocated for cassandra. See more info using
```
cd open_elevation/cassandra_io
./scripts/start_cassandra.sh --help
```

Before starting a second cassandra node, wait till the start up on
`node1` is over. Check if the startup is complete by saying
```
docker logs cassandra_storage | grep 'Startup complete'
```

To start the second cassandra node on the `node2` say
```
cd open_elevation/cassandra_io
./scripts/start_cassandra.sh --broadcast=10.0.0.2 --seed=10.0.0.1
```

### Setting up a worker/webserver node

Before starting with worker/webserver nodes a docker image should be
present. To build the docker image say
```
scripts/build_docker.sh
```

Alternatively, a docker image can be pulled from the docker image
repository:
```
...
```

A series of configuration parameters should be set up in the
`configs/pvgrip.conf` file depending your network configuration.

The following specifies address of one of the cassandra nodes
```
[cassandra]
ip = 10.0.0.2
```
as well as the address of the message broker node
```
[redis]
ip = 10.0.0.1
```

The following line sets the interface where the webserver and message
broker ports are binding:
```
[server]
interface = pvgrip0
```

The following line
```
scripts/run_docker.sh
```
starts the webserver, the broker and the processing nodes.

Say
```
scripts/run_docker.sh ./scripts/start.sh --what=minion
```
to start only the processing node.

## Data

The server utilises [Cassandra storage](https://cassandra.apache.org/)
that stores distributed data and spatial index.

### Uploading data

Unlike the original project multiple sources of data can be used. If
multiple data sources are available for a given query, a dataset with
higher resolution is selected.

To make some data available for a webserver it is needed to be
uploaded. The data can be a collection of geotiff raster files (in
arbitrary coordinate system) or a directory containing
`remote_meta.json` file, specifying required information needed to
query remote data.

To upload a new data, it should be placed as a subdirectory of the
`data/current` directory. To upload data say
```
./scripts/upload_data.py data/current/<new data>
```
This will build an index and upload necessary data to the cassandra
storage.

```
./scripts/upload_data.py data/current/
```
will process all data placed in the `data/current` directory.

All data should be placed in `data/current` directory.

### Preprocessing raster files

Say
```
scripts/preprocess.sh
```
to preprocess data. That command converts splits files on smaller
chunks (maximum is 3000x3000 pixels).

Some parts of the scripts depends on the version of GDAL being used
(and also requires GDAL installed on the host machine). Hence, it is
possible to run the scripts from inside the docker image
```
scripts/run_docker.sh scripts/preprocess.sh
```

Preprocess script create a backup of the data using the
```
cp -rl data/current data/current.bak
```

### Workers cache

When data is queried from the cassandra storage, it is cached to a
local node disk. The amount of the local cache is controlled by the
following option in the `configs/pvgrip.conf`
```
[cache]
limit_worker = 10
# amount in GB
```

### Data replication

The replication in the cassandra is controlled by the following
options in the `configs/pvgrip.conf`
```
[cassandra]
replication = SimpleStrategy
replication_args = {"replication_factor": 1}
```
The replication_args must be a valid json string.

See more information on the replication in the cassandra
[here](https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/architecture/archDataDistributeReplication.html).


## Querying data

Here we assume that a machine has access to the webserver running on
`10.0.0.1`.

The server timeout is 30 seconds. In this time it either response with
a binary file or a json dictionary.

In case task is running the following message is returned:
```
{"message": "task is running"}
```

### Query help

On a node with an access to the webserver
```
> curl 10.0.0.1:8080/api/help
```
to see all available commands.

To query help on any particular command call
```
> curl 10.0.0.1:8080/api/raster/help
```

### NRW Lidar and Aerial data

The server gives access to [geospatial
datasets](https://www.opengeodata.nrw.de/produkte/geobasis/) provided
by NRW.

For the a special file `remote_meta.json` should be specified in the
corresponding directory. See examples in `templates` directory.

For example, for the Lidar data:
```
{
    "root_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_%s_%s_1_nw.laz",
    "step": 1000,
    "box_resolution": 1,
    "epsg": 25832,
    "box_step": 1,
    "pdal_resolution": 0.3,
    "meta_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/index.json",
    "meta_entry_regex": "^3dm_32_(.*)_(.*)_1_nw.*$",
    "las_stats": ["max","min","count","mean","idw","stdev"],
    "if_compute_las": "yes"
}
```
where `pdal_resolition` indicated that Lidar data is computed with a
resolution of 30cm, resolution is given in terms of EPSG:25832, and
hence in meters.

From a cloud point several statistic can be computed: `min`, `max` and
`count`. Those functions are taken for every region with a specified
resolution. See more info on available statistics
[here](https://pdal.io/stages/writers.gdal.html#writers-gdal).

If `if_compute_las` is not `"yes"`, then `las_stats` and
`pdal_resolution` arguments are ignored.

### Sample raster images

It is possible to query a coordinate from a bounding box. For example,
```
> curl 10.0.0.1:8080/api/raster\?box="\[50.7731,6.0793,50.7780,6.0882\]" -o output_fn
```
the box argument is given either as a list (in POST query) or as a json string list (in GET query).

See
```
> curl 10.0.0.1:8080/api/raster/help
```
to see default options.

### Shadows

To obtain shadows say
```
> curl 10.0.0.1:8080/api/shadow\?box="\[50.6046,6.38,50.6098,6.3977\]"\&timestr="2020-05-01_5:3:00"
```
time must be UTC.

As usual
```
> curl 10.0.0.1:8080/api/shadow/help
```
gives some help.

To compile a shadow video use `scripts/shadow_movie/query_data.sh`
script. It also allow to stress test the server.

## Caveats

 - Be aware that all floating arguments are cached with 8 digit
   accuracy. Hence querying box `[50,6,51,7]` and
   `[50.0000001,6,51,7]` will yield the same results

 - Some jobs require a tree of operations to be completed. Two queries
   producing: "message: task is running" does not imply that the
   second query will complete its job. It means that the second query
   hit a task dependency that is already being run for the first
   query. This might require running query more than 2 times.

 - The value of a pixel of a raster image is selected as a maximum of
   all available data for this point. In case data is still missing in
   all datasets, the data is taken as a nearest neighbour from the
   generated raster image.

   Hence for more accurate shadows it is important to specify correct
   `data_re` argument.

 - For any non-laz remote data one has to specify stat="". For
   example, this applies for the aerial images:
   ```
   curl 10.0.0.1:8081/api/raster\?box="\[50.6053,6.3835,50.6085,6.3922\]"\&step="1"\&data_re=".*_Aerial"\&output_type="png"\&stat="" -o test.png
   ```
