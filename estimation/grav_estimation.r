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
nb_tirages  <-  1024
N <- nrow(mat_rang)
K <- ncol(mat_rang)
mat_names <- dimnames(mat_rang)
shufs <- matrix(NA, ncol = N, nrow = nb_tirages)
for (i in 1:nb_tirages)  shufs[i, ] <- sample(N, size = N)

list_param <- list(
  rkdist = mat_rang, 
  emplois = les_emplois$emplois_reserves_c, 
  actifs = les_actifs$act_18_64c, 
  f = les_actifs$fuite, 
  shuf = shufs, 
  nthreads = 16)
list_param_na <- list_param
list_param_na$rkdist <- mat_rang_na

load(file= "output/dist.com.srda")

gc()

source("estimation/f.flux gravitaire.r")

kl_grav <- function(dist, fuite, hab, emp, delta, tol = 0.0001, 
                    furness=FALSE, version = 2, communaliser = TRUE) {
  if(furness)
    flux <- flux_grav_furness(dist, fuite, hab, emp, delta, tol)
  else
    flux <- flux_grav(dist, fuite, hab, emp, delta)
  dimnames(flux) <- dimnames(dist)
  if(communaliser)
    flux_c <- communaliser1(list(emps = flux), 1) 
  else
  {
    flux <- as_tibble(flux, row.names = "COMMUNE") |> 
      pivot_longer(cols=-COMMUNE, names_to = "DCLT", values_to  = flux) |> 
      left_join(mobpro, by= c("COMMUNE", "DCLT"))
    flux_c <- list(flux = flux$flux, mobpro = flux$mobpro)}
  kl_f <- switch(version,
                 "1" = kullback_leibler,
                 "2" = kullback_leibler_2,
                 "3" = kullback_leibler_3,
                 "4" = \(x,y) r2kl2(x,y)$r2kl)
  kl_f(flux_c$flux, flux_c$mobpro)
}

score_grav <- function(dist, fuite, hab, emp, delta, tol = 0.0001, furness = FALSE) {
  if(furness)
    f <- flux_grav_furness(dist, fuite, hab, emp, delta, tol)
  else
    f <- flux_grav(dist, fuite, hab, emp, delta)
  f <- communaliser1(list(emps = f))
  sum((f$mobpro - f$flux)^2)
}

### test ---
m <- flux_grav_furness(mat_distance_na, 
                       les_actifs$fuite,
                       les_actifs$act_18_64c,
                       les_emplois$emplois_reserves_c,
                       10 )
communaliser1(list(emps=m))
m2 <- flux_grav(dist = mat_distance_na, 
                fuite = les_actifs$fuite,
                hab = les_actifs$act_18_64c,
                emp = les_emplois$emplois_reserves_c,
                delta = 50)
dimnames(m2) <- dimnames(mat_distance_na)
communaliser1(list(emps=m2))
kl_grav(dist = mat_distance_na, 
        fuite = les_actifs$fuite,
        hab = les_actifs$act_18_64c,
        emp = les_emplois$emplois_reserves_c,
        delta = 10, version=4,
        furness = FALSE)

fkl <- optimise(
  \(x) kl_grav(
    dist=mat_distance_na,
    fuite=les_actifs$fuite,
    hab=les_actifs$act_18_64c,
    emp=les_emplois$emplois_reserves_c,
    delta=x, furness=TRUE, version=4),
  lower = 1,
  upper = 100, 
  maximum=TRUE)

fkl_nf <- optimise(
  \(x) kl_grav(
    dist=mat_distance_na,
    fuite=les_actifs$fuite,
    hab=les_actifs$act_18_64c,
    emp=les_emplois$emplois_reserves_c,
    delta=x, furness=FALSE, version=4),
  lower = 1,
  upper = 100, 
  maximum=TRUE)

fref <- flux_grav_furness(
  mat_distance_na, les_actifs$fuite, les_actifs$act_18_64c, 
  les_emplois$emplois_reserves_c,
  delta=fkl$maximum)
fref_nf <- flux_grav(
  mat_distance_na, les_actifs$fuite, les_actifs$act_18_64c, 
  les_emplois$emplois_reserves_c,
  delta=fkl_nf$maximum)
fref_c <- communaliser1(list(emps=fref)) |> 
  mutate(alg = "gravitaire avec furness",
         n_est = 1) |> 
  filter(flux !=0)
fref_c_nf <- communaliser1(list(emps=fref_nf)) |> 
  mutate(alg = "gravitaire sans furness",
         n_est = 1)  |> 
  filter(flux !=0)
grav_furness <- list(
  bf = fref_c,
  stats = fref_c |> comparer_meaps_bygrp(), 
  itors=NULL,
  par = fkl
)
qsave(grav_furness, file = "output/grav_furness.qs")

grav_nonfurness <- list(
  bf = fref_c_nf,
  stats = fref_c_nf |> comparer_meaps_bygrp(), 
  itors=NULL,
  par = fkl_nf
)
qsave(grav_nonfurness, file = "output/grav_nonfurness.qs")

# fluxg <- emp_flux(s1, fref)$s |>
#   as_tibble() |>
#   rename_all(~str_c("e",.x)) |> 
#   mutate(gh = str_c("h", s1$hgroupes$g)) |>
#   relocate(gh) |> 
#   add_total() |> 
#   rowwise() |> 
#   mutate(total = sum(c_across(2:4))) |> 
#   ungroup() |> 
#   mutate(across(2:5, ~prettyNum(round(.x), format ="d", big.mark = " ")))