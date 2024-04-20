setwd("~/marseille")
library(qs, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(rmeaps)
library(arrow)
library(r3035)
library(data.table)
library(duckdb)
library(furrr)
library(tictoc)
library(sf)
library(tidyverse)
library(Matrix)
source("secrets/azure.R")
calc <- TRUE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
load("baselayer.rda")

cli::cli_alert_info("Flux")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

time <- open_dataset(time_dts) |> to_duckdb() |> select(fromidINS, toidINS, t) |> collect()
froms <- distinct(time, fromidINS) |> pull() |> as.character()
tos <- distinct(time, toidINS) |> pull() |> as.character()

communes <- c200ze |> filter(scot) |> mutate(com = as.integer(com)) |> pull(com, name = idINS) 
communes <- communes[froms]
dclts <- c200ze |> filter(emp_resident>0) |> mutate(com = as.integer(com)) |> pull(com, name = idINS) 
dclts <- dclts[tos]

mobpro <- qs::qread(mobpro_file) |> filter(mobpro95) |> group_by(COMMUNE, DCLT) |> summarize(mobpro = sum(NB))
masses_AMP <- bd_read("AMP_masses")
actifs <- masses_AMP$actifs[froms]
emplois <- masses_AMP$emplois[tos]
fuite <- masses_AMP$fuites[froms]/actifs

COMs <- tibble(actifs = actifs, 
               fuite = fuite,
               from = froms,
               COMMUNE = communes )
DCLTs <- tibble(emplois = emplois, 
                to = tos, DCLT = dclts)

N <- length(actifs)
K <- length(emplois)

nshuf <- 64

shufs <- emiette(les_actifs = actifs, nshuf = nshuf, seuil = 500)

if(calc) {
  estimation <- bd_read("estimation")
  
  meaps <- meaps_continu(dist = time, 
                         emplois = emplois, 
                         actifs = actifs, 
                         f = fuite, 
                         shuf = shufs,
                         attraction = estimation |> slice(2) |> pull(method),
                         param = estimation |> slice(2) |> pull(param) |> pluck(1),
                         nthreads = 3L)
  
  arrow::write_parquet(meaps, "{mdir}/meaps/meaps.parquet" |> glue())
} 

meaps <- arrow::open_dataset("{mdir}/meaps/meaps.parquet" |> glue()) |> 
  to_duckdb() |> 
  rename(f_ij = flux)

# meaps.c <- communaliser(meaps, communes, dclts)

# joining ----

delta <- open_dataset("/space_mounts/data/marseille/delta_iris") |> 
  to_duckdb() |> 
  mutate(all = bike+walk+transit+car) |> 
  select(fromidINS, toidINS, car, all)

# distances.car <- open_dataset(dist_dts) |> 
#   to_duckdb() |> 
#   filter(mode %in% c("car_dgr")) |>
#   select(fromidINS, toidINS, travel_time, distance)
# 
# distances.transit <- open_dataset(dist_dts) |> 
#   to_duckdb() |> 
#   filter(mode %in% c("transit")) |>
#   anti_join(distances.car, by=c("fromidINS", "toidINS")) |> 
#   select(fromidINS, toidINS, travel_time, distance)

meaps.joined <- meaps |> 
  left_join(delta, by = c("fromidINS", "toidINS"))

meaps.joined <- meaps.joined |> 
  filter(car>0, all>0) |> 
  mutate(km_car_ij= f_ij * car, 
         km_ij = f_ij * all) |> 
  mutate(co2_ij = km_car_ij * 218/1000000)

meaps_from <- meaps.joined |> 
  group_by(fromidINS) |> 
  summarize(
    km_i = sum(km_ij, na.rm=TRUE),
    co2_i = sum(co2_ij, na.rm=TRUE),
    f_i = sum(f_ij, na.rm=TRUE)) |> 
  mutate(
    km_pa = km_i/f_i,
    co2_pa = co2_i/f_i) |> 
  collect()
meaps_from <- meaps_from |> 
  as_tibble() |> 
  left_join(c200ze |> select(fromidINS = idINS, com), by='fromidINS') |> 
  st_as_sf()

meaps_to <- meaps.joined |> 
  group_by(toidINS) |> 
  summarize(
    km_j = sum(km_ij, na.rm=TRUE),
    co2_j = sum(co2_ij, na.rm=TRUE),
    f_j = sum(f_ij, na.rm=TRUE)) |> 
  mutate(
    km_pe = km_j/f_j,
    co2_pe = co2_j/f_j) |> 
  collect()

meaps_to <- meaps_to |> 
  as_tibble() |> 
  left_join(c200ze |> select(toidINS = idINS, com, scot), by='toidINS') |> 
  st_as_sf()

decor_carte <- bd_read("decor_carte")
decor_carte_large <- bd_read("decor_carte_large")
version <- "4.424"

carte_co2_to <- ggplot() +
  decor_carte +
  geom_sf(
    data= meaps_to |> filter(scot),
    mapping= aes(fill=co2_pe), col=NA) + 
  scale_fill_distiller(
    type = "seq",
    palette = "RdBu",
    name = "CO2/an/emploi") +
  theme_ofce_void()

tension <- arrow::read_parquet("{mdir}/meaps/tension_odds_d.parquet" |> glue()) |>
  mutate(toidINS = as.integer(toidINS),
         tension = (max(rang)-rang)/(max(rang)-min(rang))) |> 
  left_join(c200ze |> select(toidINS = idINS, com, scot), by='toidINS') |> 
  st_as_sf() |> 
  mutate(tens = santoku::chop_deciles(tension))

carte_tension <- ggplot() +
  decor_carte +
  geom_sf(
    data= tension |> filter(scot),
    mapping= aes(fill=tens), col=NA) + 
  scale_fill_brewer(
    type = "seq",
    palette = "Blues",
    name = "Tension sur l'emploi") +
  theme_ofce_void()

carte_co2_from <- ggplot() +
  decor_carte +
  geom_sf(
    data= meaps_from,
    mapping= aes(fill=co2_pa), col=NA) + 
  scale_fill_viridis_c(
    option="plasma",
    direction=-1,
    name = "Emissions de CO2\ntCO2/an/emploi") +
  theme_ofce_void()


bd_write(meaps_from)
bd_write(meaps_to)
bd_write(carte_co2_from)
bd_write(carte_co2_to)
bd_write(carte_tension)
bd_write(version)
