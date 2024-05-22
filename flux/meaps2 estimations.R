setwd("~/marseille")
library(qs, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(rmeaps)
library(arrow)
library(r3035)
library(duckdb)
library(furrr)
library(tictoc)
library(sf)
library(tidyverse)
library(Matrix)
source("secrets/azure.R")
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
load("baselayer.rda")

time_ranked_group <- qs::qread("/space_mounts/data/marseille/meaps/trg.qs")

msoc64 <- meaps_optim(time_ranked_group, attraction = "marche_liss",
                   meaps_fun = "multishuf",
                   parametres = c(10, 10), upper= c(20, 100))
qs::qsave(msoc64, "/space_mounts/data/marseille/meaps/est_msoc64.qs")
gc()

ai <- meaps_optim(time_ranked_group, attraction = "marche_liss",
                      meaps_fun = "all_in",
                      parametres = c(10, 20), upper= c(20, 100))
qs::qsave(ai, "/space_mounts/data/marseille/meaps/est_ai.qs")
gc()

bench::mark(meaps <- all_in_grouped(
  time_ranked_group, attraction = "marche_liss", 
  parametres = c(10, 25)))

arrow::write_parquet(meaps, "/space_mounts/data/marseille/meaps/meaps_est_msoc64.parquet")

meaps <- all_in(
  time_ranked_group, attraction = "marche_liss", 
  parametres = qs::qread("/space_mounts/data/marseille/meaps/est_ai.qs")$par)

arrow::write_parquet(meaps, "/space_mounts/data/marseille/meaps/meaps_est_ai.parquet")

meaps <- arrow::open_dataset("/space_mounts/data/marseille/meaps/meaps_est_msoc64.parquet") |> 
  to_duckdb() |> 
  rename(f_ij = flux) |> 
  mutate(fromidINS = as.integer(fromidINS),
         toidINS = as.integer(toidINS))
gc()

# joining ----

delta <- open_dataset("/space_mounts/data/marseille/delta_iris") |> 
  to_duckdb() |> 
  mutate(all = bike+walk+transit+car) |> 
  select(fromidINS, toidINS, car, all)

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

carte_co2_to <- ggplot() +
  decor_carte +
  geom_sf(
    data= meaps_to |> filter(scot),
    mapping= aes(fill=co2_pe), col=NA)+ 
  scale_fill_viridis_c(
    limits = c(0,3.5),
    option="plasma",
    direction=-1,
    name = "CO2/an/emploi")

carte_co2_from <- ggplot() +
  decor_carte +
  geom_sf(
    data= meaps_from,
    mapping= aes(fill=co2_pa), col=NA) + 
  scale_fill_viridis_c(
    limits = c(0,3.5),
    option="plasma",
    direction=-1,
    name = "Emissions de CO2\ntCO2/an/emploi")

bd_write(carte_co2_from)
bd_write(carte_co2_to)
bd_write(meaps_from)
bd_write(meaps_to)
