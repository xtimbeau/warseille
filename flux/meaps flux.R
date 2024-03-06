library(qs, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(rmeaps)
library(arrow)
library(r3035)
library(data.table)
library(duckdb)
library(furrr)
source("secrets/azure.R")
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

arrow::set_cpu_count(8)

# ---- Definition des zones ----
cli::cli_alert_info("lecture de baselayer dans {.path {getwd()}}")
load("baselayer.rda")

cli::cli_alert_info("trajets")
data.table::setDTthreads(4)
arrow::set_cpu_count(4)

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com)
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com)

rm <- qs::qread(rank_matrix)
froms <- rownames(rm)
tos <- colnames(rm)
masses_AMP <- bd_read("AMP_masses")

N <- length(masses_AMP$actifs)
K <- length(masses_AMP$emplois)
nb_tirages <- 64
shufs <- matrix(NA, ncol = N, nrow = nb_tirages)
for (i in 1:nb_tirages) shufs[i, ] <- sample(N, size = N)
modds <- matrix(1L, nrow=N, ncol=K)
dimnames(modds) <- dimnames(rm)
library(tictoc)
tic();meaps <- meaps_multishuf(rkdist = rm, 
                emplois = masses_AMP$emplois, 
                actifs = masses_AMP$actifs, 
                f = masses_AMP$fuites, 
                shuf = shufs, 
                nthreads = 8, 
                modds = modds); toc()








  walk(algs_meaps$alg, ~{
  gc()
  
  modds <- arrow::read_parquet("/scratch/estimations_meaps/{.x}.parquet" |> glue())
  rn <- modds |> pull(from)
  modds <- modds |> 
    select(-from) |> 
    as.matrix()
  rownames(modds) <- rn
  
  nb_tirages <- 256
  shufs <- matrix(NA, ncol = N, nrow = nb_tirages)
  for (i in 1:nb_tirages) shufs[i, ] <- sample(N, size = N)
  
  list_param <- list(
    rkdist = mat_rang_na, 
    emplois = les_emplois$emplois_reserves_c, 
    actifs = les_actifs$act_18_64c, 
    f = les_actifs$fuite, 
    shuf = shufs, 
    nthreads = 16, 
    modds = modds,
    progress=FALSE)
  
  meaps <- do.call(meaps_multishuf, list_param)
  dimnames(meaps) <- dimnames(mat_rang)
  meaps <- as.data.table(meaps, keep.rownames = TRUE)
  meaps <- melt(meaps, id.vars="rn")
  setnames(meaps, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
  meaps <- meaps[f_ij>0, ]
  dis <- merge(distances, meaps, by = c("fromidINS","toidINS"), all.x = TRUE)
  dis <- dis[f_ij>0, ]
  dis[ , actif_i := sum(f_ij, na.rm=TRUE), by= "fromidINS"]
  dis[, nbtrajets_par_ind_ij := trajet_pa * (f_ij / actif_i) ]
  
  dis <- dis[, .(
    id,
    tc,
    Kbcl,
    proba_bike,
    proba_car,
    proba_transit,
    proba_walk,
    f_ij,
    trajet_pa,
    nbtrajets_par_ind_ij)]
  
  nx <- str_replace_all(.x,"%","") |> 
    str_replace_all("é", "e") |> 
    str_replace_all(" ", "_")
  promeaps_file <- "{repository_distances}/proba_pro_meaps_{nx}.parquet" |> glue()
  cli::cli_alert_success("écriture de {.path {promeaps_file}}")
  arrow::write_parquet(dis, promeaps_file)
}, .progress = TRUE)

source("estimation/f.flux gravitaire.r")
pwalk(algs_grav, ~{
  gc()
  
  furness <- str_detect(..1, "avec furness")
  
  list_param <- list(
    dist = mat_distance_na, 
    emp = les_emplois$emplois_reserves_c, 
    hab = les_actifs$act_18_64c, 
    fuite = les_actifs$fuite, 
    delta = ..5,
    furness = furness)
  
  flux <- do.call(flux_grav, list_param)
  dimnames(flux) <- dimnames(mat_rang)
  flux <- as.data.table(flux, keep.rownames = TRUE)
  flux <- melt(flux, id.vars="rn")
  setnames(flux, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
  flux <- flux[f_ij>0, ]
  dis <- merge(distances, flux, by = c("fromidINS","toidINS"), all.x = TRUE)
  dis <- dis[f_ij>0, ]
  dis[ , actif_i := sum(f_ij, na.rm=TRUE), by= "fromidINS"]
  dis[, nbtrajets_par_ind_ij := trajet_pa * (f_ij / actif_i) ]
  
  dis <- dis[, .(
    id,
    tc,
    Kbcl,
    proba_bike,
    proba_car,
    proba_transit,
    proba_walk,
    f_ij,
    trajet_pa,
    nbtrajets_par_ind_ij)]
  
  nx <- str_replace_all(.x,"%","") |> 
    str_replace_all("é", "e") |> 
    str_replace_all(" ", "_")
  promeaps_file <- "{repository_distances}/proba_pro_meaps_{nx}.parquet" |> glue()
  cli::cli_alert_success("écriture de {.path {promeaps_file}}")
  arrow::write_parquet(dis, promeaps_file)
}, .progress = TRUE)

t_toc <- toc(TRUE, TRUE)
timer <- tolower(lubridate::seconds_to_period(round(t_toc$toc-t_toc$tic)))

# if(nrow(probas[is.na(proba_car),])>0) { 
#   cli::cli_alert_danger(
#     "{nrow(probas)} paires d'o/d de probabilités avec {nrow(probas[is.na(proba_car),])} nas, en {timer}")
# } else {
#   cli::cli_alert_success(
#     "{nrow(probas)} paires d'o/d de probabilités pas de na, en {timer}")}


rm(list=ls(all.names=TRUE))
gc(reset=TRUE)
