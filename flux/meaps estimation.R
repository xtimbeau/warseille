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

arrow::set_cpu_count(8)

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

dp <- rmeaps::prep_meaps_dist(time, 
                              emplois=emplois, actifs=actifs,
                              fuite=fuite, shuf=shufs,
                              groups_from = communes,
                              groups_to = dclts)

rm(time)

tic();ref <- rmeaps::meaps_optim(dp, progress=TRUE); toc()
ref <- ref |> 
  left_join(mobpro, by=c("COMMUNE", "DCLT")) 
estimation <-  tibble(
  method = "reference",
  param = NA,
  kl = kullback_leibler(ref$flux, ref$mobpro),
  nshuf = nrow(dp$shuf),
  convergence = NA,
  message = NA, 
  flux = list(ref))

attraction <- "marche_liss"
alg <- function(p) {
  mod <- rmeaps::meaps_optim(
    prep = dp,
    attraction = attraction,
    param = c(p[[1]], p[[2]]),
    nthreads = 16L) 
  mod <- mod |> left_join(mobpro, by=c("COMMUNE", "DCLT"))
  kl <- kullback_leibler(mod$flux, mod$mobpro)
  cli::cli_alert_info("p: {signif(p,4)}  ; kl: {signif(kl,4)}")
  return(kl)
}

p_alg <- optim(c(8,10), alg, 
               method ="L-BFGS-B",
               lower = c(0, 0), upper=c(100, 100))

estimation <- bind_rows(
  estimation, 
  tibble(
    method = attraction,
    param = list(p_alg$par),
    kl = p_alg$value, 
    nshuf = nrow(dp$shuf),
    convergence = p_alg$convergence,
    message = p_alg$message,
    flux = list(meaps_optim(dp, attraction = attraction, param=p_alg$par))))

bd_write(estimation)
