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
calc <- FALSE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
load("baselayer.rda")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

time_d <- open_dataset(time_dts) |> 
  to_duckdb() |> 
  filter(mobpro95, t <= 2000) |> 
  select(fromidINS, toidINS, metric=t) |>
  collect() |> 
  arrange(fromidINS, metric, toidINS)

froms <- distinct(time_d, fromidINS) |> pull() |> as.character() |> sort()
tos <- distinct(time_d, toidINS) |> pull() |> as.character() |> sort()

communes <- c200ze |> filter(scot) |> pull(com, name = idINS) 
communes <- communes[froms]
dclts <- c200ze |> filter(emp_resident>0) |> pull(com, name = idINS) 
dclts <- dclts[tos]

masses_AMP <- bd_read("AMP_masses")
actifs <- masses_AMP$actifs[froms]
emplois <- masses_AMP$emplois[tos]
fuites <- masses_AMP$fuites[froms]/actifs
fuites[fuites==0] <- 0.0001
actifs <- actifs*sum(emplois)/sum(actifs*(1-fuites))

mobpro <- qs::qread(mobpro_file) |>
  filter(mobpro95) |>
  filter(COMMUNE %in% unique(communes), DCLT %in% unique(dclts)) |> 
  group_by(COMMUNE, DCLT) |>
  summarize(mobpro = sum(NB), .groups = "drop") |> 
  rename(group_from = COMMUNE, group_to = DCLT, value = mobpro)

COMs <- tibble(actifs = actifs, 
               fuite = fuites,
               from = froms,
               COMMUNE = communes )
DCLTs <- tibble(emplois = emplois, 
                to = tos, DCLT = dclts)

# nouvelle version !!
time_ranked <- meapsdata(
  triplet = time_d,
  actifs = actifs, emplois = emplois, fuites = fuites, nshuf = 64, seuil = 500)

time_ranked_group <- meapsdatagroup(
  time_ranked, 
  group_from = communes,
  group_to = dclts,
  cible = mobpro)

qs::qsave(time_ranked_group, "/space_mounts/data/marseille/meaps/trg.qs")
