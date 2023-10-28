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
progressr::handlers(global = TRUE)
progressr::handlers("cli")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

# ds <- open_dataset("DVFdata", partitioning = c("Communes"))

resol <- 200

c200ze <- qs::qread("{c200ze_file}" |> glue()) |> 
  filter(pze)

origines <- c200ze |>
  filter(ind>0, scot) |> 
  select(ind) |>
  st_centroid() 
opportunites <- c200ze |>
  filter(emp>0, mobpro99) |> 
  select(emplois=emp) |> 
  st_centroid() |> 
  st_transform(crs=4326)

message(str_c("Nombre de carreaux sur la zone",
              "résidents = {nrow(origines)}",
              "opportunités emploi = {nrow(opportunites)}",
              "opportunités complètes = {c200ze |> filter(emp>0|ind>0) |> nrow()}",
              sep = "\n") |> glue())

# Choix du jour du transit
jour_du_transit <- plage('~/files/localr5/') |> choisir_jour_transit()
message(
  "jour retenu: \n{lubridate::wday(jour_du_transit, label = TRUE, abbr = FALSE)} {jour_du_transit}" |> glue())

# ---- CALCUL DE L'ACCESSIBILITE ----
## transit --------------
future::plan("multisession", workers=1)

r5_transit <- routing_setup_r5(path = '~/files/localr5/', date=jour_du_transit, n_threads = 16,
                               breakdown = TRUE)

iso_transit_dt <- iso_accessibilite(quoi = opportunites, 
                                    ou = origines, 
                                    resolution = resol,
                                    tmax = 120, 
                                    chunk = 1e+6,
                                    pdt = 1,
                                    dir = "transit", 
                                    routing = r5_transit,
                                    ttm_out = TRUE,
                                    future=TRUE)


arrow::write_parquet(ttm_idINS(iso_transit_dt), sink="~/files/transit_ref.parquet" |> glue())

## dodgr car ----------------------
# téléchargement du OSM en format silicate
# c'est enregistré, donc on peut passer si c'est  déjà fait

if(FALSE) {
  zone <- qs::qread(communes_ar_file) |> 
    filter(mobpro99) |> 
    summarize()
  osm <- download_osmsc(zone, elevation = TRUE, workers = 16)
  qs::qsave(osm, glue("{mdir}/osm.qs"))
  unlink(glue("{mdir}/dodgr"), recursive=TRUE)
  dir.create(glue("{mdir}/dodgr"), recursive = TRUE)
  file.copy(glue("{mdir}/osm.qs"),
            to = glue("{mdir}/dodgr/marseille.scosm"),
            overwrite = TRUE)
  rm(osm)
  gc()
}

dodgr_router <- routing_setup_dodgr(glue("{mdir}/dodgr/"), 
                                    mode = "CAR", 
                                    turn_penalty = TRUE,
                                    distances = TRUE,
                                    elevation = TRUE,
                                    n_threads = 8L,
                                    overwrite = TRUE,
                                    nofuture = FALSE)
plan("multisession", workers = 4L)

unlink("{mdir}/temp_dodgr" |> glue(), recursive = TRUE)
cardgr <- iso_accessibilite(quoi = opportunites, 
                            ou = origines, 
                            resolution = 200,
                            tmax = 120, 
                            pdt = 1,
                            dir = glue("{mdir}/temp_dodgr"),
                            routing = dodgr_router,
                            ttm_out = TRUE,
                            future=TRUE)

ttt <- accessibility::ttm_idINS(cardgr)
arrow::write_parquet(ttt, glue("{dir_dist}/car.parquet"))

rm(cardgr, ttt)
gc()

## dodgr bike ----------------------

dodgr_bike <- routing_setup_dodgr(glue("{mdir}/dodgr/"), 
                                  mode = "BICYCLE", 
                                  turn_penalty = TRUE,
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 8L,
                                  overwrite = FALSE,
                                  nofuture=FALSE)

plan("multisession", workers = 4L)

unlink("{mdir}/temp_bikedgr" |> glue(), recursive = TRUE)
bikedgr <- iso_accessibilite(quoi = opportunites, 
                             ou = origines, 
                             resolution = 200,
                             tmax = 120, 
                             pdt = 1,
                             dir = glue("{mdir}/temp_bikedgr"),
                             routing = dodgr_bike,
                             ttm_out = TRUE,
                             future=TRUE)

ttt <- accessibility::ttm_idINS(bikedgr)

arrow::write_parquet(ttt, glue("{dir_dist}/bike.parquet"))

## dodgr walk ----------------------

dodgr_walk <- routing_setup_dodgr(glue("{mdir}/dodgr/"), 
                                  mode = "WALK", 
                                  turn_penalty = TRUE,
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 8L,
                                  overwrite = FALSE,
                                  nofuture  = FALSE)

plan("multisession", workers = 4L)

unlink("{mdir}/temp_walkdgr" |> glue(), recursive = TRUE)
walkdgr <- iso_accessibilite(quoi = opportunites, 
                             ou = origines, 
                             resolution = 200,
                             tmax = 120, 
                             pdt = 1,
                             dir = glue("{mdir}/temp_walkdgr"),
                             routing = dodgr_walk,
                             ttm_out = TRUE,
                             future=TRUE)

ttt <- accessibility::ttm_idINS(walkdgr)

arrow::write_parquet(ttt, glue("{dir_dist}/walk.parquet"))

## marche à pied --------------
# passer à 8*16 = 128 vCPU
future::plan("multisession", workers=1)
rJava::.jinit(silent=TRUE)
logger::log_threshold("INFO")

r5_walk <- routing_setup_r5(path = '~/files/localr5/', 
                            date=jour_du_transit, 
                            n_threads = 8,
                            mode = "WALK",
                            overwrite = TRUE, 
                            di=TRUE, 
                            elevation="TOBLER", 
                            max_rows=50000,
                            max_rides = 1)

iso_walk_dt <- iso_accessibilite(quoi = opportunites, 
                                 ou = c200.scot_tot, 
                                 resolution = resol,
                                 tmax = 90, 
                                 pdt = 1, chunk=1e+6,
                                 dir = "walk",
                                 routing = r5_walk,
                                 ttm_out = TRUE,
                                 future = FALSE)

arrow::write_parquet(ttm_idINS(iso_walk_dt), sink="~/files/walk.parquet" |> glue())

## voiture r5 --------------
# passer à 8*15 = 120 vCPU
# ca prend 20h à calculer (20h*120vCPU = 100j vCPU) pour la Rochelle
future::plan("multisession", workers=1)
rJava::.jinit(silent=TRUE)
logger::log_threshold("INFO")

r5_car5 <- routing_setup_r5(path = '~/files/localr5/', 
                            date=jour_du_transit, 
                            n_threads = 16, 
                            mode = "CAR",
                            overwrite = TRUE, 
                            di=TRUE,
                            max_rows=100000,
                            max_rides = 1)

iso_car5_dt <- iso_accessibilite(quoi = opportunites, 
                                 ou = c200.scot_tot, 
                                 resolution = resol,
                                 tmax = 120, 
                                 pdt = 1,
                                 chunk = 1e+7,
                                 dir = "car5",
                                 routing = r5_car5,
                                 ttm_out = TRUE,
                                 future = FALSE)


arrow::write_parquet(ttm_idINS(iso_car5_dt), sink="~/files/car5.parquet" |> glue())

# dodgr pour la voiture --------------------------------------------------------
# apparement il pourrait pas marcher en futur mais on est pas sur et on sait pas pourquoi
# on peut repasser à 16 vCPU
# c'est (beaucoup) plus rapide, mais est ce mieux ?
logger::log_threshold("INFO")
car_dodgr <- routing_setup_dodgr(path = "{localdata}/dodgr" |> glue(), 
                                 mode = "CAR", n_threads = 16, distances=TRUE, turn_penalty = FALSE, 
                                 wt_profile_file = "{localdata}/dodgr/dodgr_profiles.json" |> glue())

iso_card_dt <- iso_accessibilite(quoi = opportunites, 
                                 ou = c200.scot_tot, 
                                 resolution = 200,
                                 tmax = 120, 
                                 pdt = 1,
                                 dir = "card", 
                                 routing = car_dodgr,
                                 ttm_out = TRUE,
                                 future=FALSE)

arrow::write_parquet(ttm_idINS(iso_card_dt), sink="~/files/card.parquet" |> glue())

## test de vitesse excessive -----------

library(r5r)
routing <- r5_bike
o <- c200.scot3 |> filter(idINS=="r200N2625000E3492800") |> mutate(id=1:n()) 
d <- opportunites |> filter(idINS=="r200N2624000E3492200") |> mutate(id=1:n())

o <- c200.scot3 |> slice_sample(n=100) |> mutate(id=1:n())|> st_transform(crs = 4326)
d <- opportunites |> slice_sample(n=100) |> mutate(id=1:n())

d2 <- tibble(id =3, lon =-0.7530465 , lat= 46.18783)


tic();trajet <- r5r::detailed_itineraries(r5r_core = routing$core,
                                          origins = o,
                                          destinations = d,
                                          mode=routing$mode,
                                          mode_egress="WALK",
                                          departure_datetime = routing$departure_datetime,
                                          max_walk_dist = routing$max_walk_dist,
                                          max_bike_dist = Inf,
                                          max_trip_duration = 600,
                                          walk_speed = routing$walk_speed,
                                          bike_speed = routing$bike_speed,
                                          max_rides = 3,
                                          max_lts = routing$max_lts,
                                          shortest_path= TRUE,
                                          n_threads = routing$n_threads,
                                          verbose=FALSE,
                                          progress=FALSE,
                                          drop_geometry = FALSE); toc()

tic();time <- r5r::travel_time_matrix(r5r_core = routing$core,
                                      origins = o,
                                      destinations = d,
                                      mode=routing$mode,
                                      mode_egress="WALK",
                                      departure_datetime = routing$departure_datetime,
                                      max_walk_dist = routing$max_walk_dist,
                                      max_bike_dist = Inf,
                                      max_trip_duration = 600,
                                      walk_speed = routing$walk_speed,
                                      bike_speed = routing$bike_speed,
                                      max_rides = 3,
                                      breakdown = TRUE,
                                      max_lts = routing$max_lts,
                                      n_threads = routing$n_threads,
                                      verbose=FALSE,
                                      progress=FALSE); toc()

tmap::qtm(bind_rows(o, d, trajet))
trajet$geometry[[1]] |>
  st_cast("MULTIPOINT") |>
  as.matrix() |>
  as_tibble() |>
  rename(lon=V1, lat=V2) |>
  st_as_sf(coords=c("lon", "lat"), crs=4326) |>
  mutate(id=1:n()) |>
  slice(21:45) |>
  qtm(dots.col="id")
# lrosm <- st_read("{localdata}/r5/lr.gpkg" |> glue())
box <- st_bbox(bind_rows(o,d) |> st_buffer(1000))
qtm(lrosm |> filter(st_intersects(lrosm, box |> st_as_sfc(), sparse=FALSE)))
