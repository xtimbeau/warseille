# init ---------------------
library(cli)
library(sf)
library(glue)
library(conflicted)
library(data.table)
library(matrixStats)
library(tictoc)
library(Rcpp)
library(r3035)
library(tidyverse)
library(ofce)
library(progressr)
library(qs)
library(rmeaps)
library(furrr)
handlers(global=TRUE)
handlers("cli")
load("baselayer.rda")
# handlers(handler_progress(format = ":bar :percent :eta", width = 80))
source("annexes/meaps2.r")
source("estimation/f.normalisation.r")
source("estimation/f.estimation.r")
sourceCpp("cpp/meaps_oddmatrix.cpp")
conflict_prefer_all("dplyr", quiet=TRUE)

## ---- data ------
if(!file.exists("output/data_LR.rda")) {
  source("annexes/zone_emplois.r")
  source("emploi et actif.r")
  save(mat_rang, mat_distance, mat_rang_na, mat_distance_na,
       les_emplois, les_actifs, mobpro, 
       file = "output/data_LR.rda")
}
load("output/data_LR.rda")
load(file= "output/dist.com.srda")

nb_tirages  <-  256
N <- nrow(mat_rang)
K <- ncol(mat_rang)
mat_names <- dimnames(mat_rang)
shufs <- emiette(les_actifs = les_actifs$act_18_64c, nshuf = 256, seuil = 20)

list_param <- list(
  rkdist = mat_rang, 
  emplois = les_emplois$emplois_reserves_c, 
  actifs = les_actifs$act_18_64c, 
  f = les_actifs$fuite, 
  shuf = shufs, 
  nthreads = 16)
list_param_na <- list_param
list_param_na$rkdist <- mat_rang_na

## boostrap et nombre de shuf ------
if(FALSE) {
  future::plan("multisession", workers = 16)
  prof_ref <- future_imap_dfr(1:nb_tirages, ~{
    pti_shuf <- shufs[.x,, drop=FALSE]
    list_param_na$shuf <- pti_shuf
    
    MOD_i <- meaps_multishuf(
      rkdist = list_param_na$rkdist,
      emplois = list_param_na$emplois,
      actifs  = list_param_na$actifs,
      modds = matrix(1, ncol = K, nrow = N),
      f = list_param_na$f,
      shuf = pti_shuf,
      nthreads = 1, 
      normalisation = TRUE,
      progress = FALSE)
    
    communaliser1(list(emps=MOD_i))  |>
      mutate(type = "nas", 
             shuf = .x)
  }, .progress = TRUE)
  qs::qsave(prof_ref, file="output/prof_ref.qs")
  cross <- expand_grid(lbst = c(64,256, 1024),
                       nbst = 1:(5*nb_tirages))
  bootstrap <- future_pmap_dfr(
    cross, 
    ~{
      samp <- sample.int(nb_tirages, ..1, replace=TRUE)
      prof_ref |> 
        filter(shuf %in% samp) |> 
        group_by(COMMUNE, DCLT) |>
        summarize(flux = mean(flux), .groups = "drop") |> 
        mutate(shuf = ..2, lbst = ..1)
    },
    .progress = TRUE, .options = furrr_options(seed=TRUE)) 
  stats <- bootstrap |> 
    left_join(prof_ref |> distinct(COMMUNE, DCLT, mobpro), by=c("COMMUNE", "DCLT")) |>
    group_by(shuf, lbst) |>
    summarize(r2kl2 = r2kl2(flux, mobpro)$r2kl)
  qs::qsave(stats, "output/bootstrap r2kl.qs")
  ggplot(stats)+
    geom_density(
      aes(x=r2kl2, y = after_stat(density),
          group=factor(lbst), col = factor(lbst), fill = factor(lbst)),
      alpha=0.25, position="identity")+
    scale_x_continuous(labels = scales::label_percent(.01))+
    theme_ofce()
}
## test de performance ------------
if(FALSE) {
  meaps_perf <- function(nthreads, ns) {
    meaps_multishuf(
      rkdist = list_param_na$rkdist,
      emplois = list_param_na$emplois,
      actifs  = list_param_na$actifs, 
      modds = matrix(1, ncol = K, nrow = N),
      f = list_param_na$f,
      shuf = list_param_na$shuf[1:ns, ],
      nthreads = nthreads, 
      normalisation = FALSE,
      progress = FALSE)
  }
  bench::mark(
    c4 = meaps_perf(4, 1024),
    c8 = meaps_perf(8, 1024),
    c16 = meaps_perf(16, 1024)
  )
  bench::mark(
    c4 = meaps_perf(4, 256),
    c8 = meaps_perf(8, 256),
    c16 = meaps_perf(16, 256)
  )
}

if(FALSE) {
  fuite_ref <- map_dfr(c(10, 15, 20, 30, 40), ~{
    list_param$filtre_fuite <- .x
    list_param_na$filtre_fuite <- .x
    
    MOD_ref_na <- do.call(
      meaps_multishuf,
      append(list_param_na, list(modds = matrix(1, ncol = K, nrow = N), progress = TRUE)))
    communaliser(list(emps=MOD_ref_na))  |>
      mutate(type = "nas", 
             ff = .x)
  }, .progress = TRUE)
  qs::qsave(fuite_ref, file="output/fuite_ref.qs")
  stats <- fuite_ref |> group_by(type, ff) |> comparer_meaps_bygrp() |> 
    pivot_longer(cols = -c(type, ff), names_to = "stat")
  ggplot(stats)+geom_point(aes(x=ff, y=value))+facet_wrap(vars(stat), scales = "free")
}

# test de emiette
if(FALSE) {
  nshuf <- 1024
  shufs_e <- emiette(les_actifs, nshuf=nshuf, var = "act_18_64c", seuil=5)
  par_e <- list_param_na
  par_e$shuf <- shufs_e
  
  future::plan("multisession", workers = 16)
  prof_ref <- future_imap_dfr(1:nshuf, ~{
    
    pti_shuf <- shufs[.x,, drop=FALSE]
    
    MOD_i <- meaps_multishuf(
      rkdist = list_param_na$rkdist,
      emplois = list_param_na$emplois,
      actifs  = list_param_na$actifs,
      modds = matrix(1, ncol = K, nrow = N),
      f = list_param_na$f,
      shuf = pti_shuf,
      nthreads = 1, 
      normalisation = TRUE,
      progress = FALSE)
    
    communaliser1(list(emps=MOD_i))  |>
      mutate(type = "nas", 
             shuf = .x)
  }, .progress = TRUE)
  
  qs::qsave(prof_ref, file="output/prof_ref_emiette.qs")
  
  cross <- expand_grid(lbst = c(64,256, 512, 1024),
                       nbst = 1:nshuf)
  bootstrap <- future_pmap_dfr(
    cross, 
    ~{
      samp <- sample.int(nshuf, ..1, replace=TRUE)
      prof_ref |> 
        filter(shuf %in% samp) |> 
        group_by(COMMUNE, DCLT) |>
        summarize(flux = mean(flux), .groups = "drop") |> 
        mutate(shuf = ..2, lbst = ..1)
    },
    .progress = TRUE, .options = furrr_options(seed=TRUE)) 
  stats_e <- bootstrap |> 
    left_join(prof_ref |> distinct(COMMUNE, DCLT, mobpro), by=c("COMMUNE", "DCLT")) |>
    group_by(shuf, lbst) |>
    summarize(r2kl2 = r2kl2(flux, mobpro)$r2kl)
  qs::qsave(stats_e, "output/bootstrap r2kl emietté.qs")
  stats <- qs::qread("output/bootstrap r2kl.qs")
  ggplot()+
    geom_density(
      data=stats,
      aes(x=r2kl2, y = after_stat(density),
          group=factor(lbst), col = factor(lbst), fill = factor(lbst)),
      alpha=0.25, position="identity")+
    geom_density(
      data=stats_e,
      aes(x=r2kl2, y = after_stat(density),
          group=factor(lbst), col = factor(lbst), fill = factor(lbst)),
      alpha=0.25, position="identity", linetype = "dashed")+
    scale_x_continuous(labels = scales::label_percent(.01))+
    theme_ofce()
}

gc()
## ---- reference ----
# la référence est le cas où les odds sont tous égaux à 1.
modds <- matrix(1, ncol = K, nrow = N)
dimnames(modds) <- mat_names
MOD_ref_na <- do.call(
  rmeaps::meaps_multishuf,
  append(list_param_na, list(modds = matrix(1, ncol = K, nrow = N))))
arrow::write_parquet(as_tibble(modds, rownames = "from"),
                     "/scratch/estimations_meaps/référence.parquet")
mob_ref_na <- communaliser1(list(emps = MOD_ref_na))
qs::qsave(mob_ref_na, "output/mob_ref_na.qs")
mob_ref_na |> comparer_meaps_bygrp()
# mob_0 ----------------
mob_0 <- mob_ref_na |> 
  arrange(desc(mobpro)) |> 
  mutate(cum=cumsum(mobpro)/sum(mobpro))

beta <- 0.25
delta <- 0.01
steps <- 100
oddmax <- 1000
oddmin <- 0.001
# un autre algorithme (2) ---------------
# assez proche mais avec quelques nuances et pour ne rien gâcher un bug corrigé.

odcrit <- 0.99
mutation <- function(data, i) {
  mutate(
    data,
    lambda = ifelse(abs(mobpro-.data[[glue('flux.{i-1}')]])<delta,
                    1, .data[[glue("thor.{i-1}")]]),
    "or.{i}" := .data[[glue("or.{i-1}")]] * lambda^beta,
    "or.{i}" := ifelse(cum>odcrit&DCLT!=COMMUNE, 1, .data[[glue("or.{i}")]]),
    "or.{i}" := case_when(
      is.na(.data[[glue("or.{i}")]]) ~ .data[[glue("or.{i-1}")]],
      .data[[glue("or.{i}")]]>oddmax ~ oddmax,
      .data[[glue("or.{i}")]]<oddmin ~ oddmin,
      TRUE ~ .data[[glue("or.{i}")]]))
}

data <- ITOR(mob_0,
             mutation = mutation, 
             name = "99%", 
             seuil_dflux2 = 0.5, 
             par = list_param_na,
             steps = steps)

modds <- creer_matrice_odds(data$bf |> select(COMMUNE, DCLT, or))
dimnames(modds) <- mat_names
arrow::write_parquet(as_tibble(modds, rownames="from"), "/scratch/estimations_meaps/99%.parquet")
qs::qsave(data, file="output/alg2.qs")

# un autre algorithme (2p) ---------------
# assez proche mais avec quelques nuances et pour ne rien gâcher un bug corrigé.

odcrit <- 0.9

mutation <- function(data,i) {
  mutate(data,
         lambda = ifelse(abs(mobpro-.data[[glue('flux.{i-1}')]])<delta,
                         1, .data[[glue("thor.{i-1}")]]),
         "or.{i}" := .data[[glue("or.{i-1}")]] * lambda^beta,
         "or.{i}" := ifelse(cum>odcrit&DCLT!=COMMUNE, 1, .data[[glue("or.{i}")]]),
         "or.{i}" := case_when(
           is.na(.data[[glue("or.{i}")]]) ~ .data[[glue("or.{i-1}")]],
           .data[[glue("or.{i}")]]>oddmax ~ oddmax,
           .data[[glue("or.{i}")]]<oddmin ~ oddmin,
           TRUE ~ .data[[glue("or.{i}")]]))
}
data <- ITOR(mob_0, mutation, "90%",
             seuil_dflux2 = 0.5, 
             par = list_param_na, steps = steps)

modds <- creer_matrice_odds(data$bf |> select(COMMUNE, DCLT, or))
dimnames(modds) <- mat_names
arrow::write_parquet(as_tibble(modds, rownames="from"), "/scratch/estimations_meaps/90%.parquet")

qs::qsave(data, file = "output/alg2p.qs")

# algorithme 3 : diagonale ---------------------

odcrit <- 1

mutation <- function(data,i) {
  data |> 
    mutate(
      lambda = ifelse(abs(mobpro-.data[[glue('flux.{i-1}')]])<delta,
                      1, .data[[glue("thor.{i-1}")]]),
      "or.{i}" := .data[[glue("or.{i-1}")]] * lambda^beta,
      "or.{i}" := ifelse(COMMUNE!=DCLT, 1, .data[[glue("or.{i}")]]),
      "or.{i}" := case_when(
        is.na(.data[[glue("or.{i}")]]) ~ .data[[glue("or.{i-1}")]],
        .data[[glue("or.{i}")]]>oddmax ~ oddmax,
        .data[[glue("or.{i}")]]<oddmin ~ oddmin,
        TRUE ~ .data[[glue("or.{i}")]]))
}

data <- ITOR(mob_0, mutation, "diagonale",
             seuil_dflux2 = 0.5, 
             par = list_param_na, steps = steps)

modds <- creer_matrice_odds(data$bf |> select(COMMUNE, DCLT, or))
dimnames(modds) <- mat_names
arrow::write_parquet(as_tibble(modds, rownames="from"), "/scratch/estimations_meaps/diagonale.parquet")

qs::qsave(data, file="output/alg3.qs")

# algorithme 4 : no limit ---------------------
# on complète tous les cas

odcrit <- 1

mutation <- function(data,i) {
  data |> 
    mutate(
      lambda = ifelse(abs(mobpro-.data[[glue('flux.{i-1}')]])<delta,
                      1, .data[[glue("thor.{i-1}")]]),
      "or.{i}" := .data[[glue("or.{i-1}")]] * lambda^beta,
      "or.{i}" := ifelse(cum>odcrit&DCLT!=COMMUNE, 1, .data[[glue("or.{i}")]]),
      "or.{i}" := case_when(
        is.na(.data[[glue("or.{i}")]]) ~ .data[[glue("or.{i-1}")]],
        .data[[glue("or.{i}")]]>oddmax ~ oddmax,
        .data[[glue("or.{i}")]]<oddmin ~ oddmin,
        TRUE ~ .data[[glue("or.{i}")]]))
}

data <- ITOR(mob_0, mutation, "100%",
             seuil_dflux2 = 0.5, par = list_param_na, steps = steps)

modds <- creer_matrice_odds(data$bf |> select(COMMUNE, DCLT, or))
dimnames(modds) <- mat_names
arrow::write_parquet(as_tibble(modds, rownames="from"), "/scratch/estimations_meaps/100%.parquet")

qs::qsave(data, file="output/alg4.qs")

# algorithme 5 : un seul coefficient pour la diagonale -----------------------
#

diag_meaps <- function(d) {
  odds_i <- mob_0 |>
    mutate( or  = ifelse(COMMUNE==DCLT, d, 1)) |> 
    select(COMMUNE, DCLT, or ) |>
    creer_matrice_odds()
  MOD_i <- do.call(meaps_multishuf, 
                   append(list_param_na, list(modds = odds_i, progress=FALSE)))
  communal <- communaliser1(list(emps=MOD_i)) 
  kl <- r2kl2(communal$flux, communal$mobpro)$r2kl
  cli_inform("d:{signif(d,4)} r2kl:{signif(kl, 4)}")
  kl
}

par_meaps_diag <- optimise(diag_meaps, lower = 1, upper=15, maximum=TRUE)

odds_i <- mob_0 |>
  mutate(
    or  = ifelse(COMMUNE==DCLT, par_meaps_diag$maximum, 1)) |> 
  select(COMMUNE, DCLT, or ) |>
  creer_matrice_odds()

dimnames(odds_i) <- mat_names
arrow::write_parquet(as_tibble(odds_i, rownames="from"), "/scratch/estimations_meaps/un en diagonale.parquet")

MOD_i <- do.call(
  meaps_multishuf, 
  append(list_param_na, list(modds = odds_i, progress=FALSE)))
communal <- communaliser1(list(emps=MOD_i)) |> 
  mutate(alg = "un en diagonale",
         n_est = 1)
communal <- list(bf = communal, stats = communal |> comparer_meaps_bygrp(), itors=NULL)
qs::qsave(communal, file = "output/alg5.qs")

# algorithme 6 : un coefficient diagonale et communes d'emploi proches ----------------------

voisins <- dist.com |> 
  filter(as.character(COMMUNE)!=DCLT) |> 
  filter(d5 <= 4000) |> 
  transmute(COMMUNE, DCLT, v=TRUE)

voisins |> 
  group_by(COMMUNE) |>
  summarize(voisins = str_c(DCLT, collapse=", ")) |>
  print(n=100)

mob_0 <- mob_0 |> 
  left_join(voisins, by=c("COMMUNE", "DCLT")) |> 
  mutate(v = if_else(is.na(v), FALSE, v))

diag_meaps2 <- function(par) {
  d1 <- max(0,par[[1]])
  d2 <- max(0,par[[2]])
  odds_i <- mob_0 |>
    mutate( or  = ifelse(
      COMMUNE==DCLT, d1,
      ifelse(v, d2, 1))) |> 
    select(COMMUNE, DCLT, or ) |>
    creer_matrice_odds()
  MOD_i <- do.call(
    meaps_multishuf, 
    append(list_param_na, list(modds = odds_i, progress=FALSE)))
  communal <- communaliser1(list(emps=MOD_i)) 
  kl <- r2kl2(communal$flux, communal$mobpro)$r2kl
  cli_inform("d1:{signif(par[[1]],4)} d2:{signif(par[[2]],4)} r2kl:{signif(kl, 4)}")
  kl
}

parmd2 <- optim(c(5, 1), diag_meaps2, control=list(fnscale = -1))

odds_i <- mob_0 |>
  mutate(  or  = ifelse(
    COMMUNE==DCLT, parmd2$par[[1]],
    ifelse(v, parmd2$par[[2]], 1))) |> 
  select(COMMUNE, DCLT, or ) |>
  creer_matrice_odds()

dimnames(odds_i) <- mat_names
arrow::write_parquet(as_tibble(odds_i, rownames="from"), "/scratch/estimations_meaps/2 en diagonale.parquet")

MOD_i <- do.call(
  meaps_multishuf, 
  append(list_param_na, list(modds = odds_i)))

communal <- communaliser1(list(emps=MOD_i)) |> 
  mutate(alg = "2 en diagonale",
         n_est = 2)
communal <- list(bf = communal, 
                 stats = communal |> comparer_meaps_bygrp(), 
                 itors=NULL, 
                 par = parmd2)

qs::qsave(communal, file = "output/alg6.qs")

# algorithme 7 : 1 coefficient fonction des distances proches -----------------------

odds_dist <- matrix(1, ncol = K, nrow = N) 
dimnames(odds_dist) <- dimnames(mat_distance)

diag_meaps3 <- function(d) {
  odds_i <- odds_dist
  # odds_i[mat_distance>=20&mat_distance<30] <- max(d[[3]], 0.001)
  odds_i[mat_distance<10] <- max(d, 0.001)
  MOD_i <- do.call(
    meaps_multishuf, 
    append(list_param_na, list(modds = odds_i, progress=FALSE)))
  communal <- communaliser1(list(emps=MOD_i)) 
  kl <- r2kl2(communal$flux, communal$mobpro)$r2kl
  cli_inform("d:{str_c(signif(d,4), collapse=', ')} r2kl:{signif(kl, 4)}")
  kl
}

parmd3 <- optimise(diag_meaps3, lower = 0.1, upper=25, maximum=TRUE)

odds_i <- odds_dist
odds_i[mat_distance<10] <- parmd3$maximum
dir.create("/scratch/estimations_meaps")
arrow::write_parquet(as_tibble(odds_i, rownames="from"), sink = "/scratch/estimations_meaps/un fonction distance.parquet")


MOD_i <- do.call(
  meaps_multishuf, 
  append(list_param_na, list(modds = odds_i, progress=FALSE)))

communal <- communaliser1(list(emps=MOD_i)) |> 
  mutate(alg = "un fonction distance",
         n_est = 1) 
communal <- list(bf = communal, 
                 stats = communal |> comparer_meaps_bygrp(), 
                 itors=NULL,
                 par = parmd3)
qs::qsave(communal, file = "output/alg7.qs")

# algorithme 8 : 1 coefficient plus d optimisé fonction des distances proches -----------------------

options(future.globals.maxSize = 1024^3)
plan("multisession", workers = 4)
diag_meaps4 <- function(d, quiet=FALSE) {
  odds_i <- matrix(1, ncol = K, nrow = N,
                   dimnames = mat_names) 
  odds_i[ mat_distance_na < max(1,d[[2]]) ]<- max(d[[1]], 0.001)
  MOD_i <- meaps_multishuf(
    rkdist = list_param_na$rkdist,
    emplois = list_param_na$emplois, 
    actifs = list_param_na$actifs, 
    f = list_param_na$f, 
    shuf = list_param_na$shuf, 
    nthreads = list_param_na$nthreads,
    modds = odds_i,
    progress=FALSE)
  communal <- communaliser1(list(emps=MOD_i)) 
  kl <- r2kl2(communal$flux, communal$mobpro)$r2kl
  if(!quiet) {
    message <- str_c("d:", 
                     str_c(signif(d,4), collapse=', '), 
                     "r2kl:",
                     signif(kl, 4))
    cli_inform(message)
  }
  kl
}
# list_param_na$nthreads <- 16
# prmd4 <- optim(c(18, 9), diag_meaps4, 
#                method ="L-BFGS-B",
#                lower = c(1, 1),
#                upper = c(30, 30),
#                control=list(fnscale = -1))
# 
# prmd41 <- optimise(\(x) diag_meaps4(c(x, 14)), lower = 0.1, upper=25, maximum=TRUE)
# prmd42 <- optimise(\(x) diag_meaps4(c(2.982, x)), lower = 1, upper=60, maximum=TRUE)

# data <- tibble(dist = 1:30, odds = 3.338, 
#                r2kl = map2_dbl(odds, dist,
#                                ~diag_meaps4(c(.x, .y, quiet=TRUE))))
# data2 <- tibble(dist = 1:30, odds = 3.338, 
#                 r2kl = map2_dbl(odds, dist,
#                                 ~diag_meaps4(c(.x, .y, quiet=TRUE))))
# data3 <- tibble(dist = 1:30, odds = 3.338, 
#                 r2kl = map2_dbl(odds, dist,
#                                 ~diag_meaps4(c(.x, .y, quiet=TRUE))))
list_param_na$nthreads <- 4
# force_grid <- expand_grid(
#   dist = 5:15,
#   odds = seq(1, 32, by=1)) |>
#   mutate(
#     sol = future_map2(
#       dist, odds, 
#       ~diag_meaps4(c(.y, .x), quiet=TRUE),
#       .progress=TRUE))
# qs::qsave(force_grid,"output/force_grid_256.qs")
# ggplot(force_grid |> unnest(sol))+
#   geom_contour_filled(aes(x=dist, y=odds, z = sol ),
#                       breaks = c(0.88, 0.89, 0.9, 0.91, 0.92, 0.93, 0.94, 0.9405, 0.941 )) +
#   theme_ofce() +
#   scale_fill_brewer(palette="PuBuGn", direction=1)
# force_grid |> unnest(sol) |> arrange(desc(sol))
# ggplot(qread("output/force_grid_128.qs") |> unnest(sol))+
#   geom_contour_filled(aes(x=dist, y=odds, z = sol ))+
#   scale_fill_brewer(palette="Spectral", direction=1)

force_brute <- tibble(
  dist = 2:25,
  sol = future_map(
    dist,  
    ~optimise(\(x) diag_meaps4(c(x, .x), quiet=TRUE), 
              lower = 0.1, upper=100, maximum=TRUE),
    .progress=TRUE))

force_brute <- force_brute |> 
  mutate(odd = map_dbl(sol, "maximum"),
         r2kl = map_dbl(sol, "objective")) |> 
  select(-sol)

qs::qsave(force_brute, "output/force_brute_256.qs")
force_brute <- qs::qread("output/force_brute_256.qs")
best_fb <- force_brute |> filter(r2kl ==max(r2kl))

odds_i <- matrix(1, ncol = K, nrow = N,
                 dimnames = dimnames(mat_distance_na))

odds_i[mat_distance_na<best_fb$dist]  <- best_fb$odd 
dir.create("/scratch/estimations_meaps")
arrow::write_parquet(as_tibble(odds_i, rownames = "from"), 
                     sink = "/scratch/estimations_meaps/distance critique.parquet")

MOD_i <- do.call(
  meaps_multishuf, 
  list_modify(list_param_na, modds = odds_i))

communal <- communaliser1(list(emps=MOD_i)) |> 
  mutate(alg = "distance critique",
         n_est = 2) 
communal <- list(
  bf = communal,
  stats = communal |> comparer_meaps_bygrp(), 
  itors=NULL, 
  par = list(par = c(best_fb$dist, best_fb$odd)))
qs::qsave(communal, file = "output/alg8.qs")