library(tidyverse)
library(glue)
library(conflicted)
library(rmeaps)
library(arrow)
library(r3035)
library(furrr)
source("secrets/azure.R")
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

arrow::set_cpu_count(8)

# ---- Definition des zones ----
load("baselayer.rda")

cli::cli_alert_info("Time matrix")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)
time_dts <- "/tmp/time_dataset"

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

froms <- c200ze |> filter(scot, ind>0) |> pull(idINS)
tos <- c200ze |> filter(emp_resident>0) |> pull(idINS)

if(!file.exists(time_dts)) {
  unlink(time_dts, force=TRUE, recursive = TRUE)
  dir.create(time_dts)
  plan("multisession", workers = 8)
  future_walk(com_geo21_scot, \(.c) {
    dir.create(str_c(time_dts, "/", .c))
    tcom <- arrow::open_dataset(dist_dts) |> 
      to_duckdb() |> 
      filter(COMMUNE==.c) |> 
      group_by(COMMUNE, fromidINS, toidINS) |>
      summarize(t = min(travel_time, na.rm=TRUE), .groups = "drop") |> 
      compute() |> 
      to_arrow() |> 
      arrow::write_parquet(str_c(time_dts, "/", .c, "/time.parquet"))
  }, .progress=TRUE)
}

time <- arrow::open_dataset("/tmp/time_dataset")
sfroms <- split(froms, floor((1:length(froms)-1)/1000))
plan("multisession", workers = 4)
lm <- future_map(sfroms, ~{
  tot <- expand_grid(fromidINS = .x, toidINS = tos) |> 
    left_join(c200ze |> st_drop_geometry() |> select(fromidINS=idINS, COMMUNE=com), by="fromidINS") |> 
    arrange(COMMUNE, fromidINS, toidINS)
  r <- arrow::open_dataset("/tmp/time_dataset") |> 
    to_duckdb() |>
    filter(fromidINS %in% .x) |> 
    select(fromidINS, toidINS, t) |> 
    collect()
  r <- tot |> left_join(r, by=c("fromidINS", "toidINS"))
  if(nrow(r)!=length(.x)*length(tos))
    cli::cli_alert_info("désalignement des données")
  matrix(r$t, nrow = length(.x), ncol = length(tos), dimnames = list(.x, tos))
}, .progress=TRUE)
tt <- do.call(rbind, lm)

tr <- matrixStats::rowRanks(tt, ties.method = "random")

qs::qsave(tt, time_matrix)
qs::qsave(tr, rank_matrix)
