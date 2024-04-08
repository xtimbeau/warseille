setwd("~/marseille")
# init ---------------
library(tidyverse)
library(tidytransit)
library(terra)
library(tictoc)
library(accessibility)
library(r3035)
library(glue)
library(conflicted)
library(arrow)

progressr::handlers(global = TRUE)
progressr::handlers("cli")

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)
arrow::set_cpu_count(16)
## globals --------------------
load("baselayer.rda")

# ds <- open_dataset("DVFdata", partitioning = c("Communes"))

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
  select(com=com, idINS, scot, emp_resident, ind, act_mobpro) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com%in%DCLTs) |> 
  filter(from | to)

origines <- idINSes |> 
  semi_join(mobpro |> distinct(COMMUNE), by = c("com"="COMMUNE")) |> 
  filter(ind>0) |> 
  mutate(lon = r3035::idINS2lonlat(idINS)$lon,
         lat = r3035::idINS2lonlat(idINS)$lat) |> 
  select(lon, lat, idINS, ind) |> 
  st_as_sf(coords = c("lon", "lat"), crs = 4326)

destinations <- idINSes |> 
  semi_join(mobpro |> distinct(DCLT), by = c("com"="DCLT")) |> 
  filter(emp_resident>0) |> 
  mutate(lon = r3035::idINS2lonlat(idINS)$lon,
         lat = r3035::idINS2lonlat(idINS)$lat) |> 
  select(lon, lat, idINS, emp_resident) |> 
  st_as_sf(coords = c("lon", "lat"), crs = 4326)

message(str_c("Nombre de carreaux sur la zone",
              "résidents = {nrow(origines)}",
              "opportunités emploi = {nrow(destinations)}",
              sep = "\n") |> glue())

# Choix du jour du transit
jour_du_transit <- plage('~/files/localr5/') |> choisir_jour_transit()
message(
  "jour retenu: \n{lubridate::wday(jour_du_transit, label = TRUE, abbr = FALSE)} {jour_du_transit}" |> glue())

# ---- CALCUL DE L'ACCESSIBILITE ----
## transit --------------
future::plan("multisession", workers=4)

r5_transit <- routing_setup_r5(
  path = '~/files/localr5/', 
  date=jour_du_transit,
  n_threads = 16,
  extended = TRUE)

iso_transit_dt <- iso_accessibilite(quoi = destinations, 
                                    ou = origines, 
                                    resolution = 200,
                                    tmax = 120, 
                                    chunk = 1e+6,
                                    pdt = 1,
                                    dir = "transit", 
                                    routing = r5_transit,
                                    ttm_out = TRUE,
                                    future=TRUE)

transit <- ttm_idINS(iso_transit_dt) |>
  mutate(fromidINS = r3035::contract_idINS(fromidINS), 
         toidINS = r3035::contract_idINS(toidINS)) |> 
  left_join(idINSes |> select(fromidINS = idINS, COMMUNE = com), by = "fromidINS") |> 
  left_join(idINSes |> select(toidINS = idINS, DCLT = com), by = "toidINS") |> 
  select(fromidINS, toidINS, travel_time, access_time, egress_time, n_rides, COMMUNE, DCLT)
rm(iso_transit_dt)
gc()
unlink("{mdir}/distances/src/transit" |> glue(), recursive=TRUE, force=TRUE)
dir.create("{mdir}/distances/src/transit" |> glue())
coms <- unique(transit$COMMUNE)
walk(coms, ~{
  data <- transit |> filter(COMMUNE==.x)
  dir.create("{mdir}/distances/src/transit/{.x}" |> glue())
  arrow::write_parquet(
    data, 
    "{mdir}/distances/src/transit/{.x}/transit.parquet" |> glue())
}, .progress=TRUE)

## transit 95 --------------
future::plan("multisession", workers=16L)
r5_transit5 <- routing_setup_r5(
  path = '~/files/localr5/', 
  date=jour_du_transit,
  percentiles = .05,
  time_window = 60L,
  montecarlo = 1L, 
  n_threads = 16,
  extended = TRUE)

iso_transit_dt <- iso_accessibilite(quoi = destinations, 
                                    ou = origines, 
                                    resolution = 200,
                                    tmax = 120, 
                                    chunk = 1e+5,
                                    pdt = 1,
                                    dir = "transit5", 
                                    routing = r5_transit5,
                                    ttm_out = TRUE,
                                    future=TRUE)

transit <- ttm_idINS(iso_transit_dt) |> 
  left_join(idINSes |> select(fromidINS = idINS, COMMUNE = com), by = "fromidINS") |> 
  left_join(idINSes |> select(toidINS = idINS, DCLT = com), by = "toidINS") |> 
  select(fromidINS, toidINS, travel_time, access_time, egress_time, n_rides, COMMUNE, DCLT)
rm(iso_transit_dt)
gc()
unlink("{mdir}/distances/src/transit5" |> glue(), recursive=TRUE, force=TRUE)
dir.create("{mdir}/distances/src/transit5" |> glue())
coms <- unique(transit$COMMUNE)
walk(coms, ~{
  data <- transit |> filter(COMMUNE==.x)
  dir.create("{mdir}/distances/src/transit5/{.x}" |> glue())
  arrow::write_parquet(
    data, 
    "{mdir}/distances/src/transit5/{.x}/transit.parquet" |> glue())
}, .progress=TRUE)
