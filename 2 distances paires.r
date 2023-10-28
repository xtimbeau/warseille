setwd("~/marseille")
# init ---------------
library(tidyverse)
library(tidytransit)
library(terra)
library(tictoc)
# remotes::install_github("OFCE/accesstars")
library(accesstars) 
library(tmap)
# pak::pak("xtimbeau/accessibility")
library(accessibility)
library(r3035)
library(glue)
library(stars)
library(data.table)
library(conflicted)
library(sitools)
library(arrow)
library(furrr)
library(ofce)
progressr::handlers(global = TRUE)
progressr::handlers("cli")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

paires <- open_dataset(paires_mobpro_dataset)

cli::cli_alert_info("Nombre de paires de carreaux sur la zone : {f2si2(nrow(paires))}")

# OSM en format silicate ------------
# c'est enregistré, donc on peut passer si c'est  déjà fait
osm_file <- glue("{mdir}/osm.qs")
if(!file.exists(osm_file)) {
  zone <- qs::qread(communes_ar_file) |> 
    filter(mobpro99) |> 
    summarize()
  osm <- download_osmsc(zone, elevation = TRUE, workers = 16)
  qs::qsave(osm, osm_file)
  rm(osm)
  gc()
}

dodgr_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                    osm = osm_file,
                                    mode = "CAR", 
                                    turn_penalty = TRUE,
                                    distances = TRUE,
                                    denivele = TRUE,
                                    n_threads = 8L,
                                    overwrite = FALSE,
                                    nofuture = TRUE)
plan("multisession", workers = 4L)

unlink("{mdir}/temp_dodgr" |> glue(), recursive = TRUE)

communes <- paires |> distinct(COMMUNE) |> collect() |> pull()

paire <- paires |> 
  filter(COMMUNE == communes[[1]]) |> 
  collect()
dodgr_pairs(od = paire |> slice(1:1000), routing = dodgr_router)
