library(qs, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(rmeaps)
library(arrow)
library(r3035)
library(data.table)
library(duckdb)
library(furrr)
library(tictoc)
source("secrets/azure.R")

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
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com)
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com)

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
nb_tirages <- 16
shufs <- emiette(les_actifs = masses_AMP$actifs, nshuf = 256, seuil = 300)
modds <- matrix(1L, nrow=N, ncol=K)
# modds[is.na(rm)] <- NA
# modds[rt<10] <- 10
dimnames(modds) <- dimnames(rm)

tic();meaps <- meaps_multishuf(rkdist = rm, 
                               emplois = masses_AMP$emplois, 
                               actifs = masses_AMP$actifs, 
                               f = masses_AMP$fuites/masses_AMP$actifs, 
                               shuf = shufs, 
                               nthreads = 4, 
                               modds = modds); toc()
qs::qsave(meaps, "{mdir}/tmp/meaps e 16t o1.qs" |> glue())
# meaps <- qs::qread("{mdir}/tmp/meaps e 16t o1.qs" |> glue())

# check -------------------
meaps.c <- communaliser(meaps, communes, dclts)
com_ar <- qs::qread(communes_ar_file)
library(tmap)
COMs |> 
  group_by(COMMUNE) |> 
  summarize(actifs = sum(actifs), fuites = sum(fuites)) |>
  left_join(tibble(flux  = rowSums(meaps.c), COMMUNE = as.integer(names(rowSums(meaps.c)))), by = "COMMUNE") |> 
  mutate(r = flux/actifs, COMMUNE = as.character(COMMUNE)) |> 
  left_join(com_ar |> select(INSEE_COM), by = c('COMMUNE' = 'INSEE_COM')) |> 
  st_as_sf() |> 
  tm_shape() + tm_borders() + tm_fill(col="r")

DCLTs |> 
  mutate(DCLT = as.character(DCLT)) |> 
  group_by(DCLT) |> 
  summarize(emplois = sum(emplois)) |>
  left_join(tibble(flux  = colSums(meaps.c), DCLT = names(colSums(meaps.c))), by = "DCLT") |> 
  mutate(r = flux/emplois) |> 
  left_join(com_ar |> select(INSEE_COM), by = c('DCLT' = 'INSEE_COM')) |> 
  st_as_sf() |> 
  filter(!st_is_empty(geometry)) |> 
  tm_shape() + tm_borders() + tm_fill(col="r")

tibble(from = froms, ai = matrixStats::rowSums2(meaps)) |> 
  left_join(c200ze |> select(from = idINS, act_mobpro)) |>
  mutate(r = ai/act_mobpro) |> 
  st_as_sf() |> 
  tm_shape() + tm_fill(col="r", style = "cont")
tibble(to = tos, ej = matrixStats::colSums2(meaps)) |> 
  left_join(c200ze |> select(to = idINS, emp_resident)) |>
  mutate(r = ej/emp_resident) |> 
  st_as_sf() |> 
  tm_shape() + tm_fill(col="r", style = "cont")

dimnames(meaps) <- dimnames(rm)
meaps.dt <- as.data.table(meaps, keep.rownames = TRUE)
meaps.dt <- melt(meaps.dt, id.vars="rn")
setnames(meaps.dt, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
meaps.dt <- meaps.dt[f_ij>0, ]
meaps.dt[, fromidINS := factor(fromidINS)]

delta <- open_dataset("/space_mounts/data/marseille/delta2") |> 
  collect() |> 
  setDT()

setkey(meaps.dt, "fromidINS", "toidINS") 
setkey(delta, "fromidINS", "toidINS") 

meaps.dt <- merge(meaps.dt, delta, by = c("fromidINS", "toidINS"))

meaps.dt[, `:=`(km_car_ij= f_ij * car, km_ij = f_ij * (bike+walk+transit+car))]
meaps.dt[, co2_ij := km_ij * 218/1000000]
meaps_from <- meaps.dt[,.(
  km_pa = sum(km_ij, na.rm=TRUE)/sum(f_ij),
  co2_pa = sum(co2_ij, na.rm=TRUE)/sum(f_ij),
  f_i = sum(f_ij, na.rm=TRUE)),
  by = "fromidINS"] 

meaps_from <- meaps_from |> 
  as_tibble() |> 
  left_join(c200ze |> select(fromidINS = idINS), by='fromidINS') |> 
  st_as_sf()
decor_carte <- bd_read("decor_carte")

ggplot() +
  decor_carte +
  geom_sf(
    data= meaps_from,
    mapping= aes(fill=km_pa), col=NA) + 
  scale_fill_distiller(
    type = "seq",
    palette = "Spectral",
    name = "CO2/an/adulte")
