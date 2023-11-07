library(qs)
library(sf)
library(glue)
library(vroom)
library(stars)
library(tidyverse)

load("baselayer.rda")

# pour télécharger OSM dernière version
if(FALSE) {
  options(timeout = max(60000*10, getOption("timeout")))
  download.file("https://download.geofabrik.de/europe/france-latest.osm.pbf", destfile = "/tmp/france-latest.osm.pbf")
}

zone <- qs::qread(communes_mb99_file) |> 
  summarize() |>
  st_buffer(5000)

rJava::.jinit()

infile <- "/tmp/france-latest.osm.pbf"

# positioning in the right directory for the shell command
current.wd <- getwd()
dir.create(pbf_rep)
setwd(pbf_rep)
outfile <- "/space_mounts/data/marseille/marseille.pbf"
box <- zone |> st_transform(4326) |> st_bbox()

osmosis_call <- glue(
  "/home/datahub/osmosis/bin/osmosis --read-pbf {infile} --bounding-box left={box$xmin} bottom={box$ymin} right={box$xmax} top={box$ymax} --write-pbf {outfile}") 

system(osmosis_call)

setwd("~/marseille")

# on met le pbf extrait dasn le dossier local de r5
# attention à ce qu'il n'en y ai qu'un
# on nettoie donc

pbfs <- fs::dir_ls("~/files/" |> glue(), regexp = "*.pbf")
gpkg <- fs::dir_ls("~/files/" |> glue(), regexp = "*.gpkg")
fs::file_delete(c(pbfs, "{localdata}/r5/network.dat" |> glue(), gpkg))
fs::file_copy("{outfile}" |> glue(), "~/files/localr5/" |> glue())
