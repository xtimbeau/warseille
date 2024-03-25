# on transforme les idINS dans les fichiers :
#   {dir_dist}/src/car_dgr, bike, walk, transit et transit5
library(arrow)
library(tidyverse)
library(r3035)
library(furrr)
load("baselayer.rda")

dts <- "{dir_dist}/src/car_dgr" |> glue()
dirs <- list.files(dts, full.names = TRUE)

walk(dirs, ~{
  pqn <- str_c(.x, "/part-0.parquet")
  read_parquet(pqn) |> 
    mutate(toId = r3035::contract_idINS(toId),
           fromId = r3035::contract_idINS(fromId)) |> 
    write_parquet(pqn)}, .progess = TRUE)


plan("multisession", workers = 4)
dts <- "{dir_dist}/src/transit" |> glue()
dirs <- list.files(dts, full.names = TRUE)

future_walk(dirs, ~{
  pqn <- str_c(.x, "/transit.parquet")
  read_parquet(pqn) |> 
    mutate(toidINS = r3035::contract_idINS(toidINS),
           fromidINS = r3035::contract_idINS(fromidINS)) |> 
    write_parquet(pqn)}, .progess = TRUE)

plan("multisession", workers = 8)
dts <- "{dir_dist}/src/transit5" |> glue()
dirs <- list.files(dts, full.names = TRUE)

future_walk(dirs, ~{
  pqn <- str_c(.x, "/transit.parquet")
  read_parquet(pqn) |> 
    mutate(toidINS = r3035::contract_idINS(toidINS),
           fromidINS = r3035::contract_idINS(fromidINS)) |> 
    write_parquet(pqn)}, .progess = TRUE)

plan("multisession", workers = 8)
dts <- "{dir_dist}/src/walk_tblr" |> glue()
dirs <- list.files(dts, full.names = TRUE)

future_walk(dirs, ~{
  pqn <- str_c(.x, "/ttm.parquet")
  read_parquet(pqn) |> 
    mutate(toId = r3035::contract_idINS(toId),
           fromId = r3035::contract_idINS(fromId)) |> 
    write_parquet(pqn)}, .progess = TRUE)

plan("multisession", workers = 8)
dts <- "{dir_dist}/src/bike_tblr" |> glue()
dirs <- list.files(dts, full.names = TRUE)

future_walk(dirs, ~{
  pqn <- str_c(.x, "/ttm.parquet")
  read_parquet(pqn) |> 
    mutate(toId = r3035::contract_idINS(toId),
           fromId = r3035::contract_idINS(fromId)) |> 
    write_parquet(pqn)}, .progess = TRUE)




## patch 2 : on les met en integer !

# on transforme les idINS dans les fichiers :
#   {dir_dist}/src/car_dgr, bike, walk, transit et transit5
library(arrow)
library(tidyverse)
library(r3035)
library(furrr)
load("baselayer.rda")

dts <- "{dir_dist}/src/car_dgr" |> glue()
dirs <- list.files(dts, full.names = TRUE)

open_dataset("{dir_dist}/src/car_dgr" |> glue()) |> 
  to_duckdb() |> 
  mutate(toId = as.integer(toId),
         fromId = as.integer(fromId)) |> 
  to_arrow() |> 
  write_dataset("{dir_dist}/src/car_dgr_patched" |> glue())

unlink("{dir_dist}/src/car_dgr" |> glue(), recursive=TRUE, force=TRUE)
file.rename("{dir_dist}/src/car_dgr_patched" |> glue(), "{dir_dist}/src/car_dgr" |> glue())

open_dataset("{dir_dist}/src/walk_tblr" |> glue()) |> 
  to_duckdb() |> 
  mutate(toId = as.integer(toId),
         fromId = as.integer(fromId)) |> 
  to_arrow() |> 
  write_dataset("{dir_dist}/src/walk_tblr_patched" |> glue())

unlink("{dir_dist}/src/walk_tblr" |> glue(), recursive=TRUE, force=TRUE)
file.rename("{dir_dist}/src/walk_tblr_patched" |> glue(), "{dir_dist}/src/walk_tblr" |> glue())

open_dataset("{dir_dist}/src/bike_tblr" |> glue()) |> 
  to_duckdb() |> 
  mutate(toId = as.integer(toId),
         fromId = as.integer(fromId)) |> 
  to_arrow() |> 
  write_dataset("{dir_dist}/src/bike_tblr_patched" |> glue())

unlink("{dir_dist}/src/bike_tblr" |> glue(), recursive=TRUE, force=TRUE)
file.rename("{dir_dist}/src/bike_tblr_patched" |> glue(), "{dir_dist}/src/bike_tblr" |> glue())

open_dataset("{dir_dist}/src/transit" |> glue()) |> 
  to_duckdb() |> 
  mutate(toidINS = as.integer(toidINS),
         fromidINS = as.integer(fromidINS)) |> 
  to_arrow() |> 
  write_dataset("{dir_dist}/src/transit_patched" |> glue())

unlink("{dir_dist}/src/transit" |> glue(), recursive=TRUE, force=TRUE)
file.rename("{dir_dist}/src/transit_patched" |> glue(), "{dir_dist}/src/transit" |> glue())

open_dataset("{dir_dist}/src/transit5" |> glue()) |> 
  to_duckdb() |> 
  mutate(toidINS = as.integer(toidINS),
         fromidINS = as.integer(fromidINS)) |> 
  to_arrow() |> 
  write_dataset("{dir_dist}/src/transit5_patched" |> glue())

unlink("{dir_dist}/src/transit5" |> glue(), recursive=TRUE, force=TRUE)
file.rename("{dir_dist}/src/transit5_patched" |> glue(), "{dir_dist}/src/transit5" |> glue())
