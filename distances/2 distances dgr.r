setwd("~/marseille")
# init ---------------
library(tidyverse)
library(accessibility)
library(r3035)
library(glue)
library(conflicted)
library(ofce)
library(arrow)

progressr::handlers(global = TRUE)
progressr::handlers("cli")

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

# OSM en format silicate ------------
# c'est enregistré, donc on peut passer si c'est  déjà fait
osm_file <- glue("{mdir}/osm.qs")
if(!file.exists(osm_file)) {
  zone <- qs::qread(communes_mb99_file) |> 
    summarize() |>
    st_buffer(5000)
  osm <- download_osmsc(zone, elevation = TRUE, workers = 16)
  qs::qsave(osm, osm_file)
  rm(osm)
  gc()
}

# mobpro
mobpro <- qs::qread(mobpro_file) |> 
  filter(mobpro95) |> 
  group_by(COMMUNE, DCLT, TRANS) |> 
  summarise(emp = sum(NB_in, na.rm=TRUE), .groups = "drop") |> 
  pivot_wider(names_from = TRANS, values_from = emp, values_fill = 0)

COMMUNEs <- mobpro |> distinct(COMMUNE) |> pull()
DCLTs <- mobpro |> distinct(DCLT) |> pull()

idINSes <- qs::qread(c200ze_file) |> 
  st_drop_geometry() |> 
  select(com=com22, idINS, scot, emp_resident, ind) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com %in%DCLTs) |> 
  filter(from | to)

# voiture ---------

car_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                  osm = osm_file,
                                  mode = "CAR", 
                                  turn_penalty = TRUE,
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 16L,
                                  overwrite = TRUE,
                                  nofuture = TRUE)

dgr_distances_by_com(idINSes, mobpro,
                     car_router, 
                     path=glue("{mdir}/distances/src/car_dgr2"),
                     clusterize = TRUE)  

# marche à pied--------------
car_distance <- arrow::open_dataset(glue::glue("{mdir}/distances/src/car_dgr2"))
pairs <- car_distance |>
  filter(distance <= 10000 ) |>
  distinct(COMMUNE, DCLT) |> 
  collect() |> 
  mutate(COMMUNE = as.character(COMMUNE))

walk_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                   osm = osm_file,
                                   mode = "WALK", 
                                   turn_penalty = TRUE,
                                   distances = TRUE,
                                   denivele = TRUE,
                                   n_threads = 16L,
                                   overwrite = TRUE,
                                   nofuture = TRUE)

dgr_distances_by_com(idINSes, 
                     pairs, 
                     walk_router,
                     path=glue("{mdir}/distances/src/walk_tblr"),
                     clusterize = TRUE)  

# vélo --------------
car_distance <- arrow::open_dataset(glue::glue("{mdir}/distances/src/car_dgr2"))
pairs <- car_distance |>
  filter(distance <= 25000 ) |>
  distinct(COMMUNE, DCLT) |> 
  collect() |> 
  mutate(COMMUNE = as.character(COMMUNE))

bike_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                   osm = osm_file,
                                   mode = "BICYCLE", 
                                   turn_penalty = TRUE,
                                   distances = TRUE,
                                   denivele = TRUE,
                                   n_threads = 16L,
                                   overwrite = TRUE,
                                   nofuture = TRUE)

dgr_distances_by_com(idINSes, 
                     pairs, 
                     bike_router,
                     path=glue("{mdir}/distances/src/bike_tblr"),
                     clusterize = TRUE)  

# pour faire les distances sans tobler, on crée des profiles vélo et marche qui n'ont pas 
# le même nom

dodgr::write_dodgr_wt_profile("distances/dodgr_profiles")
prof  <- readLines("distances/dodgr_profiles.json")
prof2  <- gsub(pattern = "bicycle", replace = "bicycle_ntblr", x = prof)
prof2  <- gsub(pattern = "foot", replace = "foot_ntblr", x = prof2)
writeLines(prof2, con="distances/dodgr_profiles.json")

car_distance <- arrow::open_dataset(glue::glue("{mdir}/distances/src/car_dgr2")) |> 
  to_duckdb()
pairs <- car_distance |>
  filter(distance <= 25000 ) |>
  distinct(COMMUNE, DCLT) |> 
  collect() |> 
  mutate(COMMUNE = as.character(COMMUNE))

bike_ntblr <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                  osm = osm_file,
                                  mode = "BICYCLE_NT", 
                                  wt_profile_file = "distances/dodgr_profiles.json",
                                  turn_penalty = TRUE,
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 16L,
                                  overwrite = TRUE,
                                  nofuture = TRUE)

dgr_distances_by_com(idINSes, 
                     pairs, 
                     bike_router,
                     path=glue("{mdir}/distances/src/bike_tblr"),
                     clusterize = TRUE)  
