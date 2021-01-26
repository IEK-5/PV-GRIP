#!/bin/env Rscript

require("ggplot2")
require("rjson")

plot_csv <- function(url, re)
{
    zip_fn <- basename(url)
    csv_fn <- gsub("_meta.zip","_nw.csv", zip_fn)
    download.file(url,zip_fn)
    unzip(zip_fn)
    data <- read.csv2(csv_fn, skip=5)
    file.remove(zip_fn)
    file.remove(csv_fn)
    data$lon <- as.numeric(gsub(re,"\\1",data$Kachelname))
    data$lat <- as.numeric(gsub(re,"\\2",data$Kachelname))
    ggplot(data, aes(y = lat, x = lon)) +
        geom_raster(aes(fill=Aktualitaet)) +
        ggtitle(url)
}

plot_json <- function(url, re)
{
    data <- fromJSON(file=url)
    data <- data['datasets'][[1]][[1]]['files'][[1]]
    data <- data.frame(matrix(
        unlist(data),ncol=3,byrow=TRUE))
    colnames(data) <- c("name","size","timestamp")

    data$size <- as.numeric(data$size)
    data$timestamp <- strptime(data$timestamp,
                               "%Y-%m-%dT%H:%M:%S")

    data$lon <- as.numeric(gsub(re,"\\1",data$name))
    data$lat <- as.numeric(gsub(re,"\\2",data$name))
    data$updated_at <- format(data$timestamp,"%Y-%m")

    print(paste("Total volume:",sum(data$size)/1024^3,"GB"))

    ggplot(data, aes(y = lat, x = lon)) +
        geom_raster(aes(fill=updated_at)) +
        ggtitle(url)
}

##

g1 <- plot_csv(
    url="https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_meta.zip",
    re="3dm_32_(.*)_(.*)_1.*")

g2 <- plot_csv(
    url="https://www.opengeodata.nrw.de/produkte/geobasis/lbi/dop/dop_jp2_f10/dop_meta.zip",
    re="dop10rgbi_32_(.*)_(.*)_1.*")

###

g3 <- plot_json(
    url = "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/index.json",
    re = "^3dm_32_(.*)_(.*)_1_nw.*$")

g4 <- plot_json(
    url = "https://www.opengeodata.nrw.de/produkte/geobasis/lbi/dop/dop_jp2_f10/index.json",
    re = "^dop10rgbi_32_(.*)_(.*)_1_nw.*$")

###

pdf("coverage.pdf", width=10, height=7)
print(g1)
print(g2)
print(g3)
print(g4)
dev.off()
