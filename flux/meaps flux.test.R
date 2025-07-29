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
calc <- FALSE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
load("baselayer.rda")
time_ranked_group <- qs::qread("/space_mounts/data/marseille/meaps/trg.qs")

bench::mark(flux0 <- multishuf_oc(time_ranked_group))
bench::mark(flux1 <- multishuf_oc_grouped(time_ranked_group, attraction = "marche_liss",
                                          parametres = c(9, 27)))
bench::mark(flux <- all_in(time_ranked_group))
bench::mark(flux2 <- all_in_grouped(time_ranked_group))
            # , attraction = "marche_liss"
                                    # parametres = c(10, 10)))

est <- meaps_optim(time_ranked_group,
                   meaps_fun = "multishuf", attraction = "marche_liss",
                   parametres = c(10, 10))

est_ai <- meaps_optim(time_ranked_group,
                   meaps_fun = "all_in", attraction = "marche_liss",
                   parametres = c(10, 20))

fluxs <- all_in(time_ranked_group, attraction = "marche_liss", parametres = est$par)
arrow::write_parquet(fluxs, "/space_mounts/data/marseille/meaps/meaps2.parquet")
meaps <- arrow::open_dataset("/space_mounts/data/marseille/meaps/meaps2.parquet") |> 
  to_duckdb() |> 
  rename(f_ij = flux) |> 
  mutate(fromidINS = as.integer(fromidINS),
         toidINS = as.integer(toidINS))

# matrice hessienne (mais sert à rien)

p0 <- c(9, 27)
hessienne <- function(p0, d = c(1, 1)) {
  fn <- function(p) multishuf_oc_grouped(time_ranked_group, attraction = "marche_liss", verbose = FALSE, parametres = p)$kl
  d2x <- map_dbl(c(-1, 0, 1), ~fn(p0 + d*c(.x,  0 )))
  d2y <- map_dbl(c(-1, 0, 1), ~fn(p0 + d*c( 0, .x )))
  dxy <- map_dbl(c(-1, 0, 1), ~fn(p0 + d*c(.x, .x )))
  matrix(map_dbl(list(d2x=d2x, dxy=dxy, dxy=dxy, d2y=d2y), ~diff(diff(.x))), ncol = 2)
}

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
  # decor_carte +
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
