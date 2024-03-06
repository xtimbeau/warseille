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
cli::cli_alert_info("lecture de baselayer dans {.path {getwd()}}")
load("baselayer.rda")

cli::cli_alert_info("trajets")
data.table::setDTthreads(4)
arrow::set_cpu_count(4)

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com)
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com)

rm <- qs::qread(rank_matrix)
rt <- qs::qread(time_matrix)
froms <- rownames(rm)
tos <- colnames(rm)
masses_AMP <- bd_read("AMP_masses")

N <- length(masses_AMP$actifs)
K <- length(masses_AMP$emplois)
nb_tirages <- 16
shufs <- matrix(NA, ncol = N, nrow = nb_tirages)
for (i in 1:nb_tirages) shufs[i, ] <- sample(N, size = N)
modds <- matrix(1L, nrow=N, ncol=K)
modds[is.na(rm)] <- NA
modds[rd<10] <- 10
dimnames(modds) <- dimnames(rm)

tic();meaps <- meaps_multishuf(rkdist = rm, 
                emplois = masses_AMP$emplois, 
                actifs = masses_AMP$actifs, 
                f = masses_AMP$fuites/masses_AMP$actifs, 
                shuf = shufs, 
                nthreads = 4, 
                modds = modds); toc()

dimnames(meaps) <- dimnames(rm)
meaps.dt <- as.data.table(meaps, keep.rownames = TRUE)
meaps.dt <- melt(meaps.dt, id.vars="rn")
setnames(meaps.dt, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
meaps.dt <- meaps.dt[f_ij>0, ]
meaps.dt[, fromidINS := factor(fromidINS)]


delta <- open_dataset(delta_dts) |> 
  collect() |> 
  setDT()

de[, km_ij := f_ij * delta]
de[, `:=`(co2_ij = km_ij * 218/1000000, 
          time_ij = fifelse(time==9999, NA_real_, km_ij / (distance_car/1000) * time / 60))]
de[, meaps := nx]
de_from <- de[,.(
  km_pa = sum(km_ij, na.rm=TRUE)/sum(f_ij),
  time_pa = sum(time_ij, na.rm=TRUE)/sum(f_ij),
  co2_pa = sum(co2_ij, na.rm=TRUE)/sum(f_ij),
  f_i = sum(f_ij, na.rm=TRUE)),
  by = c("mode","fromidINS", "meaps")] 


kmco2_delta <- rbind(kmco2_delta, de_from) 