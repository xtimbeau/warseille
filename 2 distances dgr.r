setwd("~/marseille")
# init ---------------
library(tidyverse)
library(accessibility)
library(r3035)
library(glue)
library(conflicted)
library(ofce)

progressr::handlers(global = TRUE)
progressr::handlers("cli")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

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
                                    overwrite = FALSE,
                                    nofuture = TRUE)


dgr_distances_by_com(idINSes, mobpro,
                     car_router, 
                     path=glue("{mdir}/distances/src/car_dgr"),
                     clusterize = TRUE)  

# marche à pied--------------

walk_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                    osm = osm_file,
                                    mode = "WALK", 
                                    turn_penalty = TRUE,
                                    distances = TRUE,
                                    denivele = TRUE,
                                    n_threads = 16L,
                                    overwrite = FALSE,
                                    nofuture = TRUE)

dgr_distances_by_com(idINSes, 
                     mobpro |> filter(walk>0), 
                     walk_router,
                     path=glue("{mdir}/distances/src/walk_dgr"),
                     clusterize = TRUE)  

# vélo --------------

bike_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                   osm = osm_file,
                                   mode = "BICYCLE", 
                                   turn_penalty = TRUE,
                                   distances = TRUE,
                                   denivele = TRUE,
                                   n_threads = 16L,
                                   overwrite = FALSE,
                                   nofuture = TRUE)

dgr_distances_by_com(idINSes, 
                     mobpro |> filter(bike>0), 
                     bike_router,
                     path=glue("{mdir}/distances/src/bike_dgr"),
                     clusterize = TRUE)  
