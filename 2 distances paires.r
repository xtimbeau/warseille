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
dodgr_pairs(od = paire |> slice(1:5000), routing = dodgr_router)

# deuxième essai, pairwise semble pas marcher

mobpro <- qs::qread(mobpro_file) |> 
  filter(mobpro99) |> 
  group_by(COMMUNE, DCLT, TRANS) |> 
  summarise(fl = any(fl),
            fw = any(fw),
            emp_scot = sum(NB_in)) |> 
  pivot_wider(names_from = TRANS, values_from = emp_scot, values_fill = 0)

iterator <- paires |> count(COMMUNE, DCLT) |> collect()

logger::log_appender(logger::appender_file("logs/log_distances"))
car_dist <- local({
  pb <- progressr::progressor(steps = sum(iterator$n))
  pb(0)
  plan("multisession", workers = 4)
  pmap_dfr(iterator, ~{
    tictoc::tic()
    les_paires <- paires |> 
      filter(COMMUNE==.x, DCLT==.y) |> 
      collect()
    from <- les_paires |> 
      distinct(fromidINS, .keep_all = TRUE) |> 
      select(id = fromidINS, lon = o_lon, lat = o_lat) |> 
      setDT()
    to <- les_paires |> 
      distinct(toidINS, .keep_all = TRUE) |> 
      select(id = toidINS, lon = d_lon, lat = d_lat) |> 
      setDT()
    ttm <- dodgr_ttm(
      o=from, 
      d=to, 
      tmax = Inf, 
      routing = dodgr_router)
    ss <- nrow(ttm$result)
    pb(amount=ss)
    time <- tictoc::toc(quiet=TRUE)
    dtime <- (time$toc - time$tic)
    speed_log <- stringr::str_c(
      ofce::f2si2(ss), "@",ofce::f2si2(ss/dtime), "p/s")
    logger::log_info(speed_log)
    ttm$result[, `:=`(COMMUNE=.x, DCLT = .y)]
  })
})
