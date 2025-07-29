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

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

time_r <- read_parquet("/space_mounts/data/marseille/distances/time_ranked")
froms <- time_r$fromidINS |> unique() |> sort()
tos <- time_r$toidINS |> unique() |> sort()

communes <- c200ze |> filter(scot) |> pull(com, name = idINS) 
communes <- as.integer(communes[froms])
dclts <- c200ze |> filter(emp_resident>0) |> pull(com, name = idINS) 
dclts <- as.integer(dclts[tos])

masses_AMP <- bd_read("AMP_masses")
actifs <- masses_AMP$actifs[froms]
emplois <- masses_AMP$emplois[tos]
fuite <- masses_AMP$fuites[froms]/actifs
check <- (sum(actifs*(1-fuite))-sum(emplois))/sum(actifs)
COMs <- tibble(actifs = actifs, 
               fuite = fuite,
               from = froms,
               COMMUNE = communes )
DCLTs <- tibble(emplois = emplois, 
                to = tos, DCLT = dclts)

N <- length(actifs)
K <- length(emplois)

p_dist <- time_r |> select(i) |> group_by(i) |> count() |> ungroup() |> pull(n) |> cumsum()
p_dist <- c(0, p_dist)

gc()
tic()
meaps <- rmeaps:::.another_meaps(time_r$j, p_dist, time_r$t, emplois, actifs, fuite, 
                                 param = 0, jr_odds = 0L, p_odds = 0L, xr_odds = 1.0,
                                 attraction = "constant",
                                 nthreads = 4)
toc()
