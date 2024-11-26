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
library(conflicted)
source("secrets/azure.R")
calc <- FALSE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
source("mglobals.r")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
communes <- c200ze |> filter(scot) |> pull(com, name = idINS) 
coms <- enframe(communes, name = "fromidINS", value = "COMMUNE") |> 
  mutate(fromidINS = as.integer(fromidINS)) |> 
  write_dataset("/tmp/coms")
coms <- open_dataset('/tmp/coms')

cc <- coms |> distinct(COMMUNE) |> collect() |> pull()

com_geo21_scot <- c200ze |> 
  filter(scot) |>
  distinct(com) |> 
  pull(com) |>
  as.integer()

com_geo21_ze <- c200ze |>
  filter(emp>0) |>
  distinct(com) |> 
  pull(com) |> 
  as.integer()

open_dataset(dist_dts) |> 
  filter(mode=="car_dgr") |> 
  select(fromidINS, toidINS, d = distance) |> 
  left_join(coms, by = "fromidINS") |> 
  group_by(COMMUNE) |> 
  arrow::write_dataset("/tmp/dist1")

dist_d <- arrow::open_dataset("/tmp/dist1")

open_dataset(time_dts) |> 
  filter(mobpro95) |> 
  select(fromidINS, toidINS, metric=t) |>
  mutate(fromidINS = as.integer(fromidINS),
         toidINS = as.integer(toidINS)) |> 
  left_join(coms, by = "fromidINS") |> 
  group_by(COMMUNE) |> 
  arrow::write_dataset(("/tmp/time1"))

time_d <- open_dataset("/tmp/time1")

fs::dir_delete("/tmp/time2")
fs::dir_create("/tmp/time2")
both_d <- map_dfr(cc, ~{
  com <- as.integer(.x)
  fs::dir_create(glue::glue("/tmp/time2/COMMUNE={com}"))
  time_d |>
    filter(COMMUNE == com) |> 
    left_join(dist_d |> filter(COMMUNE == com) |> select(-COMMUNE),
              by = c("fromidINS", "toidINS")) |> 
    mutate(fromidINS = as.character(fromidINS),
           toidINS = as.character(toidINS)) |> 
    collect() |> 
    write_parquet(sink = glue::glue("/tmp/time2/COMMUNE={com}/part.parquet"))
}, .progress=TRUE) 

# both_d <- open_dataset("/tmp/time2") |> collect() 

both_d <- both_d |>
  arrange(fromidINS, metric, toidINS)
gc()

froms <- distinct(both_d, fromidINS) |> pull() |> as.character() |> sort()
tos <- distinct(both_d, toidINS) |> pull() |> as.character() |> sort()

communes <- communes[froms]
dclts <- c200ze |> filter(emp_resident>0) |> pull(com, name = idINS) 
dclts <- dclts[tos]

mobpro <- qs::qread(mobpro_file) |>
  filter(mobpro95) |>
  filter(COMMUNE %in% unique(communes), DCLT %in% unique(dclts)) |> 
  group_by(COMMUNE, DCLT) |>
  summarize(mobpro = sum(NB), .groups = "drop") |> 
  rename(group_from = COMMUNE, group_to = DCLT, value = mobpro)

masses <- bd_read("masses")
actifs <- masses$actifs[froms]
emplois <- masses$emplois[tos]
fuites <- masses$fuites[froms]/actifs
fuites[fuites==0] <- 0.0001
actifs <- actifs*sum(emplois)/sum(actifs*(1-fuites))

# masses_mv <- bd_read("masses_mv")
# actifs_mv <- masses_mv$actifs[froms]
# emplois_mv <- masses_mv$emplois[tos]
# fuites_mv <- masses_mv$fuites[froms]/actifs_mv
# fuites_mv[fuites_mv==0] <- 0.0001
# actifs_mv <- actifs_mv*sum(emplois_mv)/sum(actifs_mv*(1-fuites_mv))

time_ranked <- meapsdata(
  triplet = both_d,
  actifs = actifs, emplois = emplois, fuites = fuites, nshuf = 64, seuil = 500)

time_ranked_group <- meapsdatagroup(
  time_ranked, 
  group_from = communes,
  group_to = dclts,
  cible = mobpro)

qs::qsave(time_ranked_group, trg_file)

rm(time_ranked, time_ranked_group)

# time_ranked_mv <- meapsdata(
#   triplet = time_d,
#   actifs = actifs_mv, emplois = emplois_mv, fuites = fuites_mv, nshuf = 64, seuil = 500)
# 
# time_ranked_mv_group <- meapsdatagroup(
#   time_ranked_mv, 
#   group_from = communes,
#   group_to = dclts,
#   cible = mobpro)
# 
# qs::qsave(time_ranked_mv_group, trgmv_file)

both_d <- both_d |> 
  filter(!is.na(d)) |> 
  rename(t = metric, metric = d) |> 
  arrange(fromidINS, metric, toidINS) 

gc()

dist_ranked <- meapsdata(
  triplet = both_d,
  actifs = actifs, emplois = emplois, fuites = fuites, nshuf = 64, seuil = 500)

dist_ranked_group <- meapsdatagroup(
  dist_ranked, 
  group_from = communes,
  group_to = dclts,
  cible = mobpro)

qs::qsave(dist_ranked_group, drg_file)
rm(dist_ranked_group, dist_ranked)

## ranked au niveau communal

both_d <- arrow::open_dataset("/tmp/time2") |> 
  to_duckdb() 
gc()

enframe(actifs, name = "fromidINS", value = "actif") |> 
  arrow::write_dataset("/tmp/dact")
dactifs <- arrow::open_dataset("/tmp/dact") |> 
  to_duckdb()

enframe(emplois, name = "toidINS", value = "emploi") |> 
  arrow::write_dataset("/tmp/demp")
demplois <- arrow::open_dataset("/tmp/demp") |> 
  to_duckdb()

enframe(dclts, name = "toidINS", value = "DCLT") |> 
  arrow::write_dataset("/tmp/ddcl")
ddclts <- arrow::open_dataset("/tmp/ddcl") |> 
  to_duckdb()
les_coms <- distinct(both_d, COMMUNE) |> collect() |> pull(COMMUNE)

coms <- coms |> 
  to_duckdb()
ccc <- coms |> distinct(COMMUNE) |> collect() |> pull() |> sort()
dcl <- ddclts |> distinct(DCLT) |> collect() |> pull() |> sort()

time_c <- map_dfr( les_coms, ~{ 
  both_d |>
    filter(COMMUNE == .x) |> 
    left_join(dactifs, by = "fromidINS") |> 
    left_join(demplois, by = "toidINS") |> 
    left_join(ddclts, by = "toidINS") |> 
    group_by(COMMUNE, DCLT) |> 
    summarize(
      d = sum(actif*emploi*d)/sum(actif*emploi),
      metric = sum(actif*emploi*metric)/sum(actif*emploi), 
      .groups = "drop") |> 
    collect()
}, .progress=TRUE)

time_communal <- time_c |> 
  rename(fromidINS = COMMUNE, toidINS = DCLT) |> 
  mutate(fromidINS = as.character(fromidINS)) |> 
  arrange(fromidINS, metric, toidINS)

fuites_com <- enframe(masses$fuites[froms], value = "fuite") |> 
  left_join(enframe(communes, value = "COMMUNE"), by="name") |> 
  group_by(COMMUNE) |> 
  summarize(fuite = sum(fuite)) |> 
  pull(fuite, name = COMMUNE)

actifs_com <- dactifs |> 
  left_join(coms, by = "fromidINS") |> 
  group_by(COMMUNE) |> 
  summarize(act = sum(actif)) |> 
  pull(act, name = COMMUNE)

emplois_com <- demplois |>
  left_join(ddclts, by = "toidINS") |> 
  group_by(DCLT) |> 
  summarize(emp = sum(emploi)) |> 
  pull(emp, name = DCLT)

time_ranked_communal <- meapsdata(
  triplet = time_communal |> 
    select(fromidINS, toidINS, metric, d) |>
    arrange(fromidINS, metric),
  actifs = actifs_com[ccc],
  emplois = emplois_com[dcl],
  fuites = fuites_com[ccc]/actifs_com[ccc], nshuf = 64, seuil = 500)

time_ranked_communal <-  meapsdatagroup(
  time_ranked_communal, 
  group_from = set_names(ccc),
  group_to = set_names(dcl),
  cible = mobpro)

qs::qsave(time_ranked_communal, trgc_file)