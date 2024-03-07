# init ---------------
setwd("~/marseille")
rm(list=ls(all.names = TRUE))
gc(reset=TRUE)
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
load("baselayer.rda")

modes <- set_names(c("walk_tblr", 'bike_tblr', 'car_dgr'))

communes <- com2021epci |> pull(INSEE_COM)

unlink(dist_dts, recursive=TRUE, force=TRUE)
dir.create(dist_dts)
plan("multisession", workers = 4)
distances <- future_walk(communes, \(commune) {
  data <- map_dfr(modes, \(mode) {
     arrow::open_dataset(str_c(dir_dist, "/src/", mode)) |>
                select(fromId, toId, travel_time, COMMUNE, DCLT, distance) |>
                # rename(travel_time_transit = travel_time) |>
                rename(fromidINS=fromId, toidINS=toId) |>
                filter(as.character(COMMUNE)==commune) |>
                collect() |>
                mutate(COMMUNE = as.character(COMMUNE), mode = mode)
    } )
  transit <- arrow::open_dataset(str_c(dir_dist, "/src/transit")) |>
    select(fromidINS, toidINS, travel_time, COMMUNE, DCLT, access_time, n_rides) |>
    # rename(travel_time = travel_time_transit) |>
    # filter(distance <= 25000 ) |>
    filter(as.character(COMMUNE)==commune) |>
    collect() |>
    mutate(COMMUNE = as.character(COMMUNE), mode='transit')
  data <- data |> bind_rows(transit)

  dir.create(str_c(dist_dts, "/", commune))
  fname <- str_c(dist_dts, "/", commune, "/allmode.parquet")
  write_parquet(data, fname)

  return(fname)
  
  }, .progress = TRUE)

rm(list=ls())
gc(reset=TRUE)