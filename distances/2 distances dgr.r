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
  select(com=com, idINS, scot, emp_resident, ind) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com%in%DCLTs) |> 
  filter(from | to)

# voiture ---------

car_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                  osm = osm_file,
                                  mode = "CAR", 
                                  turn_penalty = TRUE,
                                  wt_profile_file = "distances/dodgr_profiles_altcar.json",
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 16L,
                                  overwrite = TRUE,
                                  nofuture = TRUE)
qs::qsave(car_router, "/space_mounts/data/marseille/distances/car_router.qs")
car_router <- qs::qread("/space_mounts/data/marseille/distances/car_router.qs")
dgr_distances_by_com(idINSes, mobpro,
                     car_router, 
                     path=glue("{mdir}/distances/src/car_dgr2"),
                     clusterize = TRUE)  

 # on patche les distances en voiture afin d'introduire un coût fixe de démarrage et 
# d'arrivée

# on ajoute 1 min au départ et à l'arrivée pour la densite la plus faible
# on ajoute 5 min pour la densité la plus élevé (en log)

gc()

car_dgr2 <- open_dataset(glue("{mdir}/distances/src/car_dgr2")) |> 
  to_duckdb()

qs::qread(c200ze_file) |> 
  mutate(dens = santoku::chop_quantiles(ind, 1:5/5),
         dens = as.numeric(dens)) |> 
  st_drop_geometry() |> 
  write_dataset("/tmp/c200ze") 
c200ze <- open_dataset("/tmp/c200ze") |> 
  to_duckdb() 

unlink(glue("{mdir}/distances/src/car_dgr"), recursive = TRUE)

car_dgr2 |> 
  left_join(c200ze |> select(fromId = idINS, from_dens = dens), by="fromId") |> 
  left_join(c200ze |> select(toId = idINS, to_dens = dens), by="toId") |> 
  mutate(
    from_cf = from_dens - 1,
    to_cf = to_dens - 1,
    travel_time_park = travel_time + from_cf + to_cf + .5 - from_cf*to_cf/4) |> 
  select(-to_cf, -from_cf, -from_dens, -to_dens) |> 
  to_arrow() |> 
  write_dataset(glue("{mdir}/distances/src/car_dgr"), partitioning = "COMMUNE")

unlink(glue("{mdir}/distances/src/car_dgr2"), recursive = TRUE)

# marche à pied--------------
car_distance <- arrow::open_dataset(glue::glue("{mdir}/distances/src/car_dgr"))
pairs <- car_distance |>
  to_duckdb() |> 
  filter(distance <= 10000 ) |>
  distinct(COMMUNE, DCLT) |> 
  mutate(COMMUNE = as.character(COMMUNE)) |> 
  collect()

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
car_distance <- arrow::open_dataset(glue::glue("{mdir}/distances/src/car_dgr"))
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
                     bike_ntblr,
                     path=glue("{mdir}/distances/src/bike_ntblr"),
                     clusterize = TRUE)  

pairs <- car_distance |>
  filter(distance <= 10000 ) |>
  distinct(COMMUNE, DCLT) |> 
  collect() |> 
  mutate(COMMUNE = as.character(COMMUNE))

walk_ntblr <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                  osm = osm_file,
                                  mode = "WALK_NT", 
                                  wt_profile_file = "distances/dodgr_profiles.json",
                                  turn_penalty = TRUE,
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 16L,
                                  overwrite = TRUE,
                                  nofuture = TRUE)

dgr_distances_by_com(idINSes, 
                     pairs, 
                     walk_ntblr,
                     path=glue("{mdir}/distances/src/walk_ntblr"),
                     clusterize = TRUE)  
