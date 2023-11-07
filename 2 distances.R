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
  select(com=com22, idINS, scot, emp_resident, ind) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com %in%DCLTs) |> 
  filter(from | to)

origines <- idINSes |> 
  semi_join(mobpro |> distinct(COMMUNE), by = c("com"="COMMUNE")) |> 
  filter(ind>0) |> 
  mutate(lon = r3035::idINS2lonlat(idINS)$lon,
         lat = r3035::idINS2lonlat(idINS)$lat) |> 
  select(lon, lat, idINS, ind)

destinations <- idINSes |> 
  semi_join(mobpro |> distinct(DCLT), by = c("com"="DCLT")) |> 
  filter(emp_resident>0) |> 
  mutate(lon = r3035::idINS2lonlat(idINS)$lon,
         lat = r3035::idINS2lonlat(idINS)$lat) |> 
  select(lon, lat, idINS, emp_resident)

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
future::plan("multisession", workers=1)

r5_transit <- routing_setup_r5(
  path = '~/files/localr5/', 
  date=jour_du_transit,
  n_threads = 16,
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

arrow::write_dataset(
  ttm_idINS(iso_transit_dt),
  path="{mdir}/distances/src/transit" |> glue())