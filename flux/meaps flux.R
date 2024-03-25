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
rt <- qs::qread(time_matrix)
froms <- rownames(rm)
tos <- colnames(rm)
communes <- c200ze |> filter(scot) |> pull(com, name = idINS) 
communes <- as.integer(communes[froms])
dclts <- c200ze |> filter(emp_resident>0) |> pull(com, name = idINS) 
dclts <- as.integer(dclts[tos])

masses_AMP <- bd_read("AMP_masses")

COMs <- tibble(actifs = masses_AMP$actifs, fuites = masses_AMP$fuites, from = froms, COMMUNE = communes )
DCLTs <- tibble(emplois = masses_AMP$emplois, to = tos, DCLT = dclts)

N <- length(masses_AMP$actifs)
K <- length(masses_AMP$emplois)
nb_tirages <- 64
shufs <- emiette(les_actifs = masses_AMP$actifs, nshuf = 256, seuil = 300)
modds <- matrix(1L, nrow=N, ncol=K)
# modds[is.na(rm)] <- NA
# modds[rt<10] <- 10
dimnames(modds) <- dimnames(rm)

if(calc) {
  tic()
  meaps <- meaps_multishuf(rkdist = rm, 
                           emplois = masses_AMP$emplois, 
                           actifs = masses_AMP$actifs, 
                           f = masses_AMP$fuites/masses_AMP$actifs, 
                           shuf = shufs, 
                           nthreads = 4, 
                           modds = modds)
  toc()
  
  dir.create("{mdir}/meaps" |> glue())
  qs::qsave(meaps, "{mdir}/meaps/meaps e 64t o1.qs" |> glue())
} else {
  meaps <- qs::qread("{mdir}/meaps/meaps e 64t o1.qs" |> glue())
}

meaps.c <- communaliser(meaps, communes, dclts)
rm(rt, rm, modds)

dimnames(meaps) <- list(froms, tos)
meaps.dt <- as.data.table(meaps, keep.rownames = TRUE)
meaps.dt <- melt(meaps.dt, id.vars="rn")
setnames(meaps.dt, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
meaps.dt <- meaps.dt[f_ij>0, ]
meaps.dt[, `:=`(fromidINS = as.integer(fromidINS),
                toidINS = as.integer(as.character(toidINS)))]
arrow::write_parquet(meaps.dt, "/tmp/meaps.parquet")
meaps <- arrow::open_dataset("/tmp/meaps.parquet") |> 
  to_duckdb()
rm(meaps.dt)
gc()
# check -------------------
if(check) {
  com_ar <- qs::qread(communes_ar_file)
  library(tmap)
  COMs |> 
    group_by(COMMUNE) |> 
    summarize(actifs = sum(actifs), fuites = sum(fuites)) |>
    left_join(tibble(flux  = rowSums(meaps.c), 
                     COMMUNE = as.integer(names(rowSums(meaps.c)))), by = "COMMUNE") |> 
    mutate(r = flux/actifs, COMMUNE = as.character(COMMUNE)) |> 
    left_join(com_ar |> select(INSEE_COM), by = c('COMMUNE' = 'INSEE_COM')) |> 
    st_as_sf() |> 
    tm_shape() + tm_borders() + tm_fill(col="r")
  
  DCLTs |> 
    mutate(DCLT = as.character(DCLT)) |> 
    group_by(DCLT) |> 
    summarize(emplois = sum(emplois)) |>
    left_join(tibble(flux  = colSums(meaps.c),
                     DCLT = names(colSums(meaps.c))), by = "DCLT") |> 
    mutate(r = flux/emplois) |> 
    left_join(com_ar |> select(INSEE_COM), by = c('DCLT' = 'INSEE_COM')) |> 
    st_as_sf() |> 
    filter(!st_is_empty(geometry)) |> 
    tm_shape() + tm_borders() + tm_fill(col="r")
  
  tibble(from = as.integer(froms), ai = matrixStats::rowSums2(meaps)) |> 
    left_join(c200ze |> select(from = idINS, act_mobpro)) |>
    mutate(r = ai/act_mobpro) |> 
    st_as_sf() |> 
    tm_shape() + tm_fill(col="r", style = "cont")
  tibble(to = as.integer(tos), ej = matrixStats::colSums2(meaps)) |> 
    left_join(c200ze |> select(to = idINS, emp_resident)) |>
    mutate(r = ej/emp_resident) |> 
    st_as_sf() |> 
    tm_shape() + tm_fill(col="r", style = "cont")
}
# joining ----

delta <- open_dataset("/space_mounts/data/marseille/delta_iris") |> 
  to_duckdb() |> 
  mutate(all = bike+walk+transit+car) |> 
  select(fromidINS, toidINS, car, all)

meaps <- left_join(meaps, delta, by = c("fromidINS", "toidINS"))

meaps <- meaps |> 
  filter(car>0, all>0) |> 
  mutate(km_car_ij= f_ij * car, 
         km_ij = f_ij * all) |> 
  mutate(co2_ij = km_car_ij * 218/1000000)

meaps_from <- meaps |> 
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

meaps_to <- meaps |> 
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
