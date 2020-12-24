#!/bin/env Rscript

require("ggplot2")
require("rjson")

##

url <- "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_meta.zip"
download.file(url,"./3dm_meta.zip")
unzip("./3dm_meta.zip")
data <- read.csv2("3dm_nw.csv", skip=5)
file.remove("./3dm_meta.zip")
file.remove("./3dm_nw.csv")
re <- "3dm_32_(.*)_(.*)_1.*"
data$lon <- as.numeric(gsub(re,"\\1",data$Kachelname))
data$lat <- as.numeric(gsub(re,"\\2",data$Kachelname))

g1 <- ggplot(data, aes(y = lat, x = lon)) +
    geom_raster(aes(fill=Aktualitaet))

###

url <- "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/index.json"
re <- "^3dm_32_(.*)_(.*)_1_nw.*$"

data <- fromJSON(file=url)
data <- data['datasets'][[1]][[1]]['files'][[1]]
data <- data.frame(matrix(
    unlist(data),ncol=3,byrow=TRUE))
colnames(data) <- c("name","size","timestamp")

data$size <- as.numeric(data$size)
data$timestamp <- strptime(data$timestamp,"%Y-%m-%dT%H:%M:%S")

data$lon <- as.numeric(gsub(re,"\\1",data$name))
data$lat <- as.numeric(gsub(re,"\\2",data$name))
data$updated_at <- format(data$timestamp,"%Y-%m")

g2 <- ggplot(data, aes(y = lat, x = lon)) +
    geom_raster(aes(fill=updated_at))

###

pdf("coverage.pdf", width=10, height=7)
print(g1)
print(g2)
dev.off()
