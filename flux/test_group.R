setwd("~/warseille")
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
library(ofce)
library(gt)
library(duckplyr)
library(futurize)

future::plan(future.mirai::mirai_multisession, workers = 4)
# source("secrets/azure.R")
calc <- FALSE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# paramètre duckdb
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, 'SET temp_directory = "/tmp"')
DBI::dbExecute(con, 'SET max_temp_directory_size = "100GB"')
DBI::dbExecute(con, 'SET memory_limit = "16GB"')
DBI::dbExecute(con, 'SET threads to 16')
DBI::dbExecute(con, 'SET preserve_insertion_order=false')

# ---- Definition des zones ----
source("mglobals.r")

communes <- qs::qread(communes_ref_file) |> 
  st_drop_geometry() |> 
  select(com = INSEE_COM, densite = LIBDENS7) |> 
  mutate(
    densite = if_else(
      str_detect(com, "^132"),
      "Grands centres urbains - Marseille",
      densite))

c200ze <- qs::qread(c200ze_file) |> 
  st_drop_geometry() |>
  select(idINS, ind, com, scot) |> 
  left_join(communes, join_by(com)) |> 
  filter(scot) |> 
  to_duckdb()

coms <- c200ze |> distinct(com) |> pull(com)

unlink("/tmp/joined2", recursive = TRUE)
transitage <- open_dataset(dist_dts) |>
  filter(mode %in% c("transit", "car_dgr")) |> 
  select(fromidINS, toidINS, tt = travel_time, at = access_time, mode, COMMUNE) |> 
  to_duckdb() |> 
  group_by(COMMUNE) |> 
  semi_join(c200ze, join_by(fromidINS == idINS)) |> 
  ungroup() |> 
  pivot_wider(names_from = mode, values_from = c(tt, at)) |> 
  rename(at = at_transit) |> 
  mutate(
    at = at/60,
    transit = if_else(is.na(tt_transit), 120, tt_transit/60),
    car = tt_car_dgr/60,
    dt = (tt_transit - tt_car_dgr)/60,
    rdt = tt_transit / tt_car_dgr) |>
  transmute(
    fromidINS, toidINS, COMMUNE,
    transitage = (at<=15) & (transit <= 90) & (dt <= 30 | rdt <= 1.5) ) |>
  group_by(COMMUNE) |> 
  compute()
meaps.joined <- open_dataset(meaps.joined_file) |>
  to_duckdb() |> 
  group_by(COMMUNE) |> 
  mutate(d = round(distance/1000,1)) |>
  filter(d<=50, d > 0) |> 
  left_join(transitage, join_by(fromidINS, toidINS)) |> 
  to_arrow() |> 
  group_by(COMMUNE) |> 
  write_parquet("/tmp/joined2")
