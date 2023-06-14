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
setwd("~/marseille")
load("baselayer.rda")
# handlers(handler_progress(format = ":bar :percent :eta", width = 80))
source("~/marseille/annexes/meaps2.r")
source("~/marseille/estimation/f.normalisation.r")
source("~/marseille/estimation/f.estimation.r")
sourceCpp("~/marseille/cpp/meaps_oddmatrix.cpp")
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

names(les_actifs)[names(les_actifs) == "act_18_64c"] <- "NB"
names(les_emplois)[names(les_emplois) == "emplois_reserves_c"] <- "NB"

inputs <- list(les_individus = les_actifs, les_opportunites = les_emplois, enquete = mobpro)


parametres <- function(les_individus, les_opportunites, enquete){
  nb_tirages  <-  1024
  N <<- nrow(mat_rang)
  K <<- ncol(mat_rang)
  shufs <- matrix(NA, ncol = N, nrow = nb_tirages)
  for (i in 1:nb_tirages)  shufs[i, ] <- sample(N, size = N)
  
  list_param <- list(
    rkdist = mat_rang, 
    dist = mat_distance,
    emplois = les_opportunites$NB, 
    actifs =les_individus$NB, 
    f = les_individus$fuite, 
    shuf = shufs, 
    mode = "continu",
    nthreads = 16)
  return(list_param)
}

na_list <- function(list_param){
  list_param_na <- list_param
  list_param_na$rkdist = mat_rang_na
  list_param_na$dist = mat_distance_na
  return(list_param_na)
}

#On multiplie le nombre d'opportunités (pour simuler une situation sans problème de capacité, on suppose des capacités très élevées de sorte qu'aucun étab ne puisse être non rempli)

#cap est un booleen qui indique si on prend en compte la capacité ou non; si ce n'est pas le cas, le nombre d'opportunites est multiplié pour avoir une saturation virtuellement impossible
build_param <- function(inputs, nocap){
  assign("list_param", parametres(inputs$les_individus, inputs$les_opportunites, inputs$enquete), envir = .GlobalEnv)
  if(nocap){
    acc <- sum(list_param$actifs)
    list_param$emplois <- list_param$emplois * acc
  }
  assign("list_param_na", na_list(list_param), envir = .GlobalEnv)
}

build_param(inputs, nocap = TRUE)
gc()

## --------- GRAV fonctions -----------

flux_grav <- function(dist, fuite, hab, emp, delta) {
  f <-  (1-fuite) * hab * exp(-dist/delta)
  f <- t(t(f)*emp)
  f1 <- f * (1-fuite) * hab / rowSums2(f, na.rm=TRUE)
  f1[is.na(f1)] <- 0
  return(f1)
}

flux_grav_furness <- function(dist, fuite, hab, emp, delta, tol=0.0001) {
  habf <- (1-fuite)*hab
  d0 <-  exp(-dist/delta)
  td0 <- t(exp(-dist/delta))
  # f <- t(t(f)*emp)
  ai0 <- 1/rowSums2(t(td0 * emp), na.rm=TRUE)
  bj0 <- rep(1,length(emp)) 
  err <- 1
  erri <- 0
  while(abs(err-erri)>tol) {
    err <- erri
    bj <- 1/colSums2( ai0 * habf * d0, na.rm=TRUE)
    part_e <- t(td0 * emp * bj)
    ai <- 1/rowSums2(part_e, na.rm=TRUE) 
    fi <- rowSums2(ai * habf * part_e, na.rm=TRUE)
    erri <- sqrt(mean((fi-habf)^2, na.rm=TRUE) )
    ai0 <- ai
    bj0 <- bj
  }
  res <- t(t(ai0 * habf * d0) * emp * bj0)
  res[is.na(res)] <- 0
  return(res)
}

kl_grav <- function(dist, fuite, hab, emp, delta, tol = 0.0001, furness=FALSE, version = 2) {
  if(furness)
    flux <- flux_grav_furness(dist, fuite, hab, emp, delta, tol)
  else
    flux <- flux_grav(dist, fuite, hab, emp, delta)
  flux_c <- communaliser2(MOD = list(emps = flux), 
                          inputs$les_individus, 
                          inputs$les_opportunites, 
                          inputs$enquete, 
                          mat_rang = list_param_na$rkdist, 1)
  kl_f <- switch(version,
                 "1" = kullback_leibler,
                 "2" = kullback_leibler_2,
                 "3" = kullback_leibler_3,
                 "4" = \(x,y) r2kl2(x,y)$r2kl)
  kl_f(flux_c$flux, flux_c$NB)
}

score_grav <- function(dist, fuite, hab, emp, delta, tol = 0.0001, furness = FALSE) {
  if(furness)
    f <- flux_grav_furness(dist, fuite, hab, emp, delta, tol)
  else
    f <- flux_grav(dist, fuite, hab, emp, delta)
  f <- communaliser1(list(emps = f))
  sum((f$mobpro - f$flux)^2)
}

## --------- GRAV est -----------

m <- flux_grav_furness(list_param_na$dist, 
                       list_param_na$f,
                       list_param_na$actifs,
                       list_param_na$emplois,
                       10 )
m <- communaliser2(MOD = list(emps = m), 
                   inputs$les_individus, 
                   inputs$les_opportunites, 
                   inputs$enquete, 
                   mat_rang = list_param_na$rkdist)

m2 <- flux_grav(dist = list_param_na$dist, 
                fuite = list_param_na$f,
                hab = list_param_na$actifs,
                emp = list_param_na$emplois,
                delta = 50)
dimnames(m2) <- dimnames(list_param_na$dist)
m2 <- communaliser2(MOD = list(emps = m2), 
                    inputs$les_individus, 
                    inputs$les_opportunites, 
                    inputs$enquete, 
                    mat_rang = list_param_na$rkdist)

kl_grav(dist = list_param_na$dist, 
        fuite = list_param_na$f,
        hab = list_param_na$actifs,
        emp = list_param_na$emplois,
        delta = 50, version=4,
        furness = FALSE)

fkl <- optimise(
  \(x) kl_grav(
    dist = list_param_na$dist, 
    fuite = list_param_na$f,
    hab = list_param_na$actifs,
    emp = list_param_na$emplois,
    delta=x, furness=TRUE, version=4),
  lower = 1,
  upper = 100, 
  maximum=TRUE)

fkl_nf <- optimise(
  \(x) kl_grav(
    dist = list_param_na$dist, 
    fuite = list_param_na$f,
    hab = list_param_na$actifs,
    emp = list_param_na$emplois,
    delta=x, furness=FALSE, version=4),
  lower = 1,
  upper = 100, 
  maximum=TRUE)

fref <- flux_grav_furness(
  list_param_na$dist, 
  list_param_na$f,
  list_param_na$actifs,
  list_param_na$emplois,
  delta=fkl$maximum)

fref_nf <- flux_grav(
  list_param_na$dist, 
  list_param_na$f,
  list_param_na$actifs,
  list_param_na$emplois,
  delta=fkl_nf$maximum)

fref_c <- communaliser2(MOD = list(emps = fref), 
                        inputs$les_individus, 
                        inputs$les_opportunites, 
                        inputs$enquete, 
                        mat_rang = list_param_na$rkdist) |> 
  mutate(alg = "gravitaire avec furness",
         n_est = 1) |> 
  filter(flux !=0)

fref_c_nf <- communaliser2(MOD = list(emps = fref_nf), 
                           inputs$les_individus, 
                           inputs$les_opportunites, 
                           inputs$enquete, 
                           mat_rang = list_param_na$rkdist) |> 
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