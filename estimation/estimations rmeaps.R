
library(qs, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(rmeaps)
library(arrow)
library(r3035)
library(duckdb)
library(furrr)
library(tictoc)
library(sf)
library(tidyverse)
library(Matrix)
library(tictoc)
source("secrets/azure.R")
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
source("mglobals.r")

tranked <- qs::qread(trg_file)
# dranked <- qs::qread(drg_file)
# tmvranked <- qs::qread(trgmv_file)
trgc <- qs::qread(trgc_file)
wdist <- trgc@triplet |> select(group_from=fromidINS, group_to = toidINS, w = d)

tic();meaps <- multishuf_oc_grouped(
  tranked, attraction="marche", parametres=c(13, 11.0455), verbose=FALSE,
  weights = wdist); toc()

tic();meaps_c <- multishuf_oc_grouped(
  trgc, attraction="marche", parametres=c(17.5, 4.94), verbose=FALSE,
  weights = wdist); toc()
tic();meaps_aic <- all_in_grouped(
  trgc, attraction="marche", parametres=c(17.5, 4.94), verbose=FALSE,
  weights = wdist); toc()

meaps <- all_in_grouped(
  tranked, attraction="marche", parametres=c(4.5, 49.34), verbose=FALSE,
  weights = wdist)

plan("multisession", workers = 4)
options('future.globals.maxSize' = 10*1024^3)

res <- meaps_optim(tranked, attraction="marche", c(5, 50), 
          version = "multishuf_oc", klway = "wlk", discret = seq(5,20, by=1),
            weights = wdist, lower = c(0, 1), upper = c(100, 150), future = FALSE)

res_ai <- meaps_optim(tranked, attraction="marche", c(5, 50), 
                   version = "all_in", klway = "wlk", discret = seq(3,15, by=.25),
                   weights = wdist, lower = c(0, 1), upper = c(100, 150), future = FALSE)

res_c <- meaps_optim(trgc, attraction="marche", c(5, 50), 
                   version = "multishuf_oc", klway = "wlk", discret = seq(3,30, by=.5),
                   weights = wdist, lower = c(0, 1), upper = c(100, 150), future = FALSE)

res_ai_c <- meaps_optim(trgc, attraction="marche", c(5, 50), 
                      version = "all_in", klway = "wlk", discret = seq(5,30, by=.5),
                      weights = wdist, lower = c(0, 1), upper = c(100, 150), future = TRUE)

res_uw <- meaps_optim(tranked, attraction="marche", c(5, 50), 
                   version = "multishuf_oc", klway = "lk", discret = seq(3,15, by=.25),
                   weights = wdist, lower = c(0, 1), upper = c(100, 150), future = FALSE)

res_ai_uw <- meaps_optim(tranked, attraction="marche", c(5, 50), 
                      version = "all_in", klway = "lk", discret = seq(3,15, by=.25),
                      weights = wdist, lower = c(0, 1), upper = c(100, 150), future = TRUE)

qs::qsave(list(res, res_ai, res_c, res_ai_c), "AMP_estimation.qs")
