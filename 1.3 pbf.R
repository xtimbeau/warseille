library(qs)
library(sf)
library(glue)
library(vroom)
library(stars)
library(tidyverse)

# pour télécharger OSM dernière version
if(FALSE) {
  fs::dir_create("/scratch/OSM")
  options(timeout = max(60*10, getOption("timeout")))
  download.file("https://download.geofabrik.de/europe/france-latest.osm.pbf", destfile = "/scratch/OSM/france-latest.osm.pbf")
}

rJava::.jinit()

load("baselayer.rda")

infile <- "france-latest.osm.pbf"

# positioning in the right directory for the shell command
current.wd <- getwd()

setwd(pbf_rep)
outfile <- pbf_file
box <- zone_emploi |> st_transform(4326) |> st_bbox()

osmosis_call <- glue(
  "osmosis --read-pbf {infile} --bounding-box left={box$xmin} bottom={box$ymin} right={box$xmax} top={box$ymax} --write-pbf {outfile}") 

system(osmosis_call)

setwd(current.wd)


# on met le pbf extrait dasn le dossier local de r5
# attention à ce qu'il n'en y ai qu'un
# on nettoie donc

pbfs <- fs::dir_ls("{localdata}/r5/" |> glue(), regexp = "*.pbf")
gpkg <- fs::dir_ls("{localdata}/r5/" |> glue(), regexp = "*.gpkg")
fs::file_delete(c(pbfs, "{localdata}/r5/network.dat" |> glue(), gpkg))
fs::file_copy("{pbf_rep}/{pbf_file}" |> glue(), "{localdata}/r5/{pbf_file}" |> glue())