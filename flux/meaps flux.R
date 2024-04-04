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
source("secrets/azure.R")
calc <- FALSE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

arrow::set_cpu_count(8)

# ---- Definition des zones ----
load("baselayer.rda")

cli::cli_alert_info("Flux")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

rm <- qs::qread(rank_matrix)
froms <- rownames(rm)
tos <- colnames(rm)
rt <- qs::qread(time_matrix)

communes <- c200ze |> filter(scot) |> pull(com, name = idINS) 
communes <- as.integer(communes[froms])
dclts <- c200ze |> filter(emp_resident>0) |> pull(com, name = idINS) 
dclts <- as.integer(dclts[tos])

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
modds <- matrix(1L, nrow=N, ncol=K)
# modds[is.na(rm)] <- NA
# modds[rt<10] <- 10
dimnames(modds) <- dimnames(rt)

if(calc) {
  tic()
  meaps <- meaps_multishuf(rkdist = rm, 
                           emplois = emplois, 
                           actifs = actifs, 
                           f = fuite, 
                           shuf = shufs, 
                           nthreads = 1, 
                           modds = modds)
  toc()
  # meaps <- qs::qread("{mdir}/meaps/meaps_64t_ofd2.qs" |> glue())
  dir.create("{mdir}/meaps" |> glue())
  rm(rm, rt, modds)
  meaps.dt <- as.data.table(meaps)
  names(meaps.dt) <- tos
  meaps.dt[, rn := froms]
  meaps.dt <- melt(meaps.dt, id.vars="rn")
  setnames(meaps.dt, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
  meaps.dt <- meaps.dt[f_ij>0, ]
  meaps.dt[, `:=`(fromidINS = as.integer(as.character(fromidINS)),
                  toidINS = as.integer(as.character(toidINS)))]
  rm(meaps)
  arrow::write_parquet(meaps.dt, "{mdir}/meaps/meaps_unif_euc.parquet" |> glue())
  rm(meaps.dt)
  gc()
} 

rm(rt, rm, modds)
gc() 

meaps <- arrow::open_dataset("{mdir}/meaps/meaps_unif_euc.parquet" |> glue()) |> 
  to_duckdb()

# meaps.c <- communaliser(meaps, communes, dclts)

# joining ----

delta <- open_dataset("/space_mounts/data/marseille/delta_iris") |> 
  to_duckdb() |> 
  mutate(all = bike+walk+transit+car) |> 
  select(fromidINS, toidINS, car, all)

distances.car <- open_dataset(dist_dts) |> 
  to_duckdb() |> 
  filter(mode %in% c("car_dgr")) |>
  select(fromidINS, toidINS, travel_time, distance)

distances.transit <- open_dataset(dist_dts) |> 
  to_duckdb() |> 
  filter(mode %in% c("transit")) |>
  anti_join(distances.car, by=c("fromidINS", "toidINS")) |> 
  select(fromidINS, toidINS, travel_time, distance)

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
bd_write(meaps_from)

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
  left_join(c200ze |> select(toidINS = idINS, com), by='toidINS') |> 
  st_as_sf()
bd_write(meaps_to)

decor_carte <- bd_read("decor_carte")
decor_carte_large <- bd_read("decor_carte_large")

carte_co2_to <- ggplot() +
  decor_carte_large +
  geom_sf(
    data= meaps_to,
    mapping= aes(fill=co2_pe), col=NA) + 
  scale_fill_distiller(
    type = "seq",
    palette = "Spectral",
    name = "CO2/an/emploi")

carte_co2_from <- ggplot() +
  decor_carte +
  geom_sf(
    data= meaps_from,
    mapping= aes(fill=co2_pa), col=NA) + 
  scale_fill_distiller(
    type = "seq",
    palette = "Spectral",
    name = "CO2/an/adulte")

bd_write(carte_co2_from)
bd_write(carte_co2_to)
