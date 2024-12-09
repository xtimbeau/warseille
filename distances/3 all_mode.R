# init ---------------
setwd("~/marseille")
library(tidyverse, quietly = TRUE, warn.conflicts = FALSE)
library(accessibility, quietly = TRUE, warn.conflicts = FALSE)
library(r3035, quietly = TRUE, warn.conflicts = FALSE)
library(glue, quietly = TRUE, warn.conflicts = FALSE)
library(data.table, quietly = TRUE, warn.conflicts = FALSE)
library(conflicted, quietly = TRUE, warn.conflicts = FALSE)
library(arrow, quietly = TRUE, warn.conflicts = FALSE)
library(tictoc, quietly = TRUE, warn.conflicts = FALSE)
library(furrr)

conflicted::conflict_prefer("filter", "dplyr", quiet=TRUE)
conflicted::conflict_prefer("select", "dplyr", quiet=TRUE)
conflicted::conflict_prefer("first", "dplyr", quiet=TRUE)

progressr::handlers(global = TRUE)
progressr::handlers(progressr::handler_progress(format = ":bar :percent :eta", width = 80))
arrow::set_cpu_count(8)

cli::cli_alert_info("lecture de baselayer dans {.path {getwd()}}")
source("mglobals.r")

modes <- set_names(c("walk_tblr", 'bike_tblr', 'car_dgr'))

communes <- qs::qread(communes_ref_file) |> 
  filter(ar)|> 
  st_drop_geometry() |> 
  filter(SIREN_EPCI %in% epci.metropole) |> 
  pull(INSEE_COM) |> sort()

unlink(dist_dts, recursive=TRUE, force=TRUE)
dir.create(dist_dts)
plan("multisession", workers = 4)
distances <- future_walk(communes, \(commune) {
  dirn <- str_c(dist_dts, "/", commune)
  dir.create(dirn)
  # car
  arrow::open_dataset(str_c(dir_dist, "/src/car_dgr")) |>
    to_duckdb() |> 
    mutate(COMMUNE = as.character(COMMUNE)) |> 
    filter(COMMUNE == commune) |> 
    select(fromId, toId, travel_time=travel_time_park, COMMUNE, DCLT, distance) |>
    rename(fromidINS=fromId, toidINS=toId) |>
    mutate(DCLT = as.character(DCLT), 
           mode = "car_dgr",
           access_time = NA_real_,
           n_rides = NA_integer_) |> 
    filter(!is.na(travel_time)) |> 
    to_arrow() |> 
    arrow::write_parquet(str_c(dirn, "/car_dgr.parquet"))
  # walk et bike
  walk(setdiff(modes, "car_dgr"), \(mode) {
    arrow::open_dataset(str_c(dir_dist, "/src/", mode)) |>
      to_duckdb() |> 
      mutate(COMMUNE = as.character(COMMUNE)) |> 
      filter(COMMUNE == commune) |> 
      select(fromId, toId, travel_time, COMMUNE, DCLT, distance) |>
      rename(fromidINS=fromId, toidINS=toId) |>
      mutate(DCLT = as.character(DCLT),
             mode = !!mode, 
             access_time = NA_real_,
             n_rides = NA_integer_) |> 
      filter(!is.na(travel_time)) |> 
      to_arrow() |> 
      arrow::write_parquet(str_c(dirn, "/", mode, ".parquet"))
  })
  # transit
  arrow::open_dataset(str_c(dir_dist, "/src/transit")) |>
    to_duckdb() |> 
    mutate(COMMUNE = as.character(COMMUNE)) |> 
    filter(COMMUNE == commune) |> 
    select(fromidINS, toidINS, travel_time, COMMUNE, DCLT, access_time, n_rides) |>
    mutate(DCLT = as.character(DCLT),
           access_time = access_time,
           n_rides = as.integer(n_rides),
           mode='transit') |> 
    filter(!is.na(travel_time)) |> 
    to_arrow() |> 
    arrow::write_parquet(str_c(dirn, "/transit.parquet"))
  
}, .progress = TRUE)

gc(reset=TRUE)
