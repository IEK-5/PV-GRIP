# Extended Open-Elevation


This is a fork of
[https://open-elevation.com](https://open-elevation.com) updated and
extended by few things I need in my project.

Below are described new commands introduced to the server. For general
references refer to the original project documentation.

## Running the server

Say
```
./build.sh
```
to build a docker image.

Say
```
./run.sh
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
