setwd("~/marseille")
library(tidyverse)
library(glue)
library(conflicted)
library(arrow)
library(r3035)
library(furrr)
source("secrets/azure.R")
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
load("baselayer.rda")
copy_files(dist_dts, "/tmp/dist_dts")
dist_dts <- "/tmp/dist_dts"
cli::cli_alert_info("Time matrix")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> 
  filter(scot) |>
  distinct(com) |>
  pull(com) |> 
  as.integer()
com_geo21_ze <- c200ze |>
  filter(emp>0) |>
  pull(com, name = idINS)

dists <- open_dataset(dist_dts) |> 
  to_duckdb()
froms <- dists |> distinct(fromidINS) |> pull() |> as.character() |> sort()
tos <- dists |> distinct(toidINS) |> pull() |> as.character() |> sort()

mobpro <- qs::qread(mobpro_file) |> 
  distinct(COMMUNE, DCLT, mobpro95)

# on fait ici le triplet de temps
# c'est ici qu'on pourrait introduire un coût généralisé
# éventuellement spécifique à chaque k-individu
if(!file.exists(time_dts)) {
  unlink(time_dts, force=TRUE, recursive = TRUE)
  dir.create(time_dts)
  plan("multisession", workers = 8)
  future_walk(com_geo21_scot, \(.c) {
    dir.create(str_c(time_dts, "/", .c))
    gc()
    tcom <- arrow::open_dataset(dist_dts) |> 
      to_duckdb() |> 
      filter(COMMUNE==.c) |> 
      group_by(fromidINS, toidINS) |>
      summarize(t = min(travel_time, na.rm=TRUE), .groups = "drop") |> 
      mutate(
        fromidINS = as.character(fromidINS),
        toidINS = as.character(toidINS)) |> 
      collect()
    tcom <- tcom |> 
      mutate(
        DCLT = as.character(com_geo21_ze[toidINS]), 
        COMMUNE = as.character(.c)) |> 
      arrange(fromidINS, t, toidINS) |> 
      left_join(mobpro, by=c("COMMUNE", "DCLT"))
      arrow::write_parquet(tcom, str_c(time_dts, "/", .c, "/time.parquet"))
  }, .progress=TRUE)
}

# n'est plus utile
# sc200 <- c200ze |> 
#   st_drop_geometry() |> 
#   select(fromidINS=idINS, COMMUNE=com)
# sfroms <- split(froms, floor((1:length(froms)-1)/1000))
# plan("multisession", workers = 4)
# lm <- future_map(sfroms, ~{
#   tot <- expand_grid(fromidINS = .x, toidINS = tos) |> 
#     left_join(sc200, by="fromidINS") 
#   r <- arrow::open_dataset(time_dts) |> 
#     to_duckdb() |>
#     filter(fromidINS %in% .x) |> 
#     select(fromidINS, toidINS, t) |> 
#     collect()
#   r <- tot |> left_join(r, by=c("fromidINS", "toidINS"))
#   if(nrow(r)!=length(.x)*length(tos))
#     cli::cli_alert_info("désalignement des données")
#   matrix(r$t, nrow = length(.x), ncol = length(tos),
#          byrow=TRUE,
#          dimnames = list(.x, tos))
# }, .progress=TRUE)
# tt <- do.call(rbind, lm)
# 
# tr <- matrixStats::rowRanks(tt, ties.method = "random")
# 
# qs::qsave(tt, time_matrix)
# qs::qsave(tr, rank_matrix)
