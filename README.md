# Extended Open-Elevation


This is a fork of
[https://open-elevation.com](https://open-elevation.com) updated and
extended by few things I need in my project.

Below are described new commands introduced to the server. For general
references refer to the original project documentation.

## Running the server

Say
```
./build_docker.sh
```
to build a docker image.

Say
```
./run_docker.sh
```
to run the image exposing port 8080.

## Data

Unlike the original project multiple sources of data can be used. If
the data for a query is duplicated, a dataset with higher resolution
is selected.

Data can be placed in several subdirectories. Only files capable of
processing by GDAL are read, other files are ignored (i.e. all
archives have to be decompressed).

Note, all data should be placed in `data/current` directory.

Say
```
./preprocess.sh
```
to preprocess data. That command converts all files to the WGS84
coordinate system, and splits files on smaller chunks.

Some parts of the scripts depends on the version of GDAL being used
(and also requires GDAL installed on the host machine). Hence, it is
possible to run the scripts from inside the docker image
```
./run.sh ./preprocess.sh
```

Preprocess script create a backup of the data using the
```
cp -rl data/current data/current.bak
```

On a active server, say
```
> curl localhost:8081/api/v1/datasets
{"results": ["data/NRW-20", "data/srtm", "data/NRW-1sec"]}
```
to query a list of directories with data.

By default data with highest resolution for each query is selected. To
select some particular data for a query use `data_re` argument. For
example,
```
curl http://localhost:8081/api/v1/lookup\?locations\=50.8793,6.1520\&data_re\='data/NRW-20'
```
where `data_re` accepts any valid regular expression (beware of the special
symbols in url).

### NRW Lidar data

The server gives access to raw [Lidar
scans](https://www.tim-online.nrw.de/tim-online2/) of the NRW regions.

To do so a certain processing is required. Server downloads data from
[the
opengeodata](https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/)
and converts point clouds to raster images. The processed data is
cached, with about 100GB allocated. If the requested region is being
processed server replies with result
```
{"message": "task is running"}
```

It takes about two minutes to process a single LAZ file, with 4
workers allocated on a server side for these tasks.

From a cloud point several statistic are computed: `min`, `max` and
`count`. Those functions are taken for every region with a specified
resolution.

The behaviour of the LAS Data directories are defined by a special
file called `las_meta.json`. For example, for the NRW data:
```
{
    "root_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_%s_%s_1_nw.laz",
    "step": 1000,
    "box_resolution": 1,
    "epsg": 25832,
    "box_step": 1,
    "fn_meta": "3dm_nw.csv",
    "pdal_resolution": 0.5,
    "meta_entry_regex": "^3dm_32_(.*)_(.*)_1_nw;.*$"
}
```
where `pdal_resolition` indicated that Lidar data is computed with a
resolution of 50cm, resolution is given in terms of EPSG:25832, and
hence in meters.

`3dm_nw.csv` file is a meta file provided by opengeodata. This list
all available las files, and is used to build the file index.
