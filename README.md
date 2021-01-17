# Extended Open-Elevation

[![Find a bird!](http://img.youtube.com/vi/GGpmm5at-a8/0.jpg)](http://www.youtube.com/watch?v=GGpmm5at-a8)

This is a fork of
[https://open-elevation.com](https://open-elevation.com) updated and
extended by few things I need in my project.

Below are described new commands introduced to the server. For general
references refer to the original project documentation.

## Running the server

Say
```
scripts/build_docker.sh
```
to build a docker image.

Say
```
scripts/run_docker.sh
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
scripts/preprocess.sh
```
to preprocess data. That command converts all files to the WGS84
coordinate system, and splits files on smaller chunks.

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

On a active server, say
```
> curl localhost:8080/api/v1/datasets
{"results": ["data/NRW-20", "data/srtm", "data/NRW-1sec"]}
```
to query a list of directories with data.

By default data with highest resolution for each query is selected. To
select some particular data for a query use `data_re` argument. For
example,
```
curl http://localhost:8080/api/v1/lookup\?locations\=50.8793,6.1520\&data_re\='data/NRW-20'
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
cached, with maximum 100GB allocated. If the requested region is being
processed server replies with result
```
{"message": "task is running"}
```

It takes about a minute to process a single LAZ file.

The behaviour of the LAS Data directories are defined by a special
file called `las_meta.json`. For example, for the NRW data:
```
{
    "root_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_%s_%s_1_nw.laz",
    "step": 1000,
    "box_resolution": 1,
    "epsg": 25832,
    "box_step": 1,
    "pdal_resolution": 0.5,
    "meta_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/index.json",
    "meta_entry_regex": "^3dm_32_(.*)_(.*)_1_nw.*$",
    "maxsize": 100,
    "las_stats": ["max","min","count"]
}
```
where `pdal_resolition` indicated that Lidar data is computed with a
resolution of 50cm, resolution is given in terms of EPSG:25832, and
hence in meters.

From a cloud point several statistic are computed: `min`, `max` and
`count` (controlled by variable `las_stats`). Those functions are
taken for every region with a specified resolution. See more info on
available statistics
[here](https://pdal.io/stages/writers.gdal.html#writers-gdal).

`maxsize` variable controls amount of saved data in GB.

### Sample raster images

It is possible to query a coordinate from a bounding box. For example,
```
> curl localhost:8080/api/v1/raster\?box="\[50.7731,6.0793,50.7780,6.0882\]"\&data_re='.*_max' -o output_fn
```
the box argument is given either as a list (in POST query) or as a json string list (in GET query).

See
```
> curl localhost:8080/api/v1/raster/help
```
to see default options.

One of the options is `output_type`. Possible values are:
 - `pickle` return a raster image as a numpy array in a pickle format
 - `geotiff` GeoTIFF raster file
 - `pnghillshade` a hillshade image in a png format

### Shadows

To obtain shadows say
```
> curl localhost:8080/api/v1/shadow\?box="\[50.6046,6.38,50.6098,6.3977\]"\&data_re='.*_max'\&timestr="2020-05-01_5:3:00"
```
time must be UTC.

As usual
```
> curl localhost:8080/api/v1/shadow/help
```
gives some help.

`output_type` can be either
 - `png` (default)
 - `geotiff` GeoTIFF raster file

## List of more technical changes

 - `rtree` index has been replaced with `open_elevation.polygon_index`

    this allows to index more complex shapes of regions

## Caveats

 - Be aware that all floating arguments are cached with 6 digit
   accuracy. Hence querying box `[50,6,51,7]` and
   `[50.0000001,6,51,7]` will yield the same results

 - Some jobs require a tree of operations to be completed. Two queries
   producing: "message: task is running" does not imply that the
   second query will complete its job. It means that the second query
   hit a task dependency that is already being run for the first
   query. This might require running query more than 2 times.

 - Between getting results try to run queries, such that all data
   (also intermediate data, like generated geotiff, etc) required for
   those queries is less than 20GB.

   This value can be extended in the settings of the RESULTS_CACHE.

 - The default timeout for the server to wait for the request is 30
   seconds. If this timeout is reach, it looks that server is
   processing something.

   In fact, under a heavy load it might take more than 30 seconds.

   Solution: do not load server too much.
