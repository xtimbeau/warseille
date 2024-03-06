library(tidyverse)
library(glue)
library(conflicted)
library(rmeaps)
library(arrow)
library(r3035)
library(furrr)

conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

arrow::set_cpu_count(8)

# ---- Definition des zones ----
cli::cli_alert_info("lecture de baselayer dans {.path {getwd()}}")
load("baselayer.rda")

cli::cli_alert_info("trajets")
data.table::setDTthreads(4)
arrow::set_cpu_count(4)
time_dts <- str_c(dir_dist, "/src/time_dataset")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com)
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com)

froms <- c200ze |> filter(scot, ind>0) |> pull(idINS)
tos <- c200ze |> filter(emp_resident>0) |> pull(idINS)

if(!file.exists(time_dts)) {
  unlink(time_dts, force=TRUE, recursive = TRUE)
  dir.create(time_dts)
  plan("multisession", workers= 4)
  future_walk(com_geo21_scot, ~{
    gc()
    dfn <- str_c(dist_dts, "/", .x, "/allmode.parquet")
    tcom <- arrow::read_parquet(dfn, 
                                as_data_frame = FALSE) |> 
      to_duckdb() |> 
      group_by(COMMUNE, fromidINS, toidINS) |>
      filter(!is.na(travel_time)) |> 
      summarize(t = min(travel_time, na.rm=TRUE), .groups = "drop") |> 
      collect()
    
    ttos <- expand_grid(fromidINS = unique(tcom$fromidINS), toidINS = tos) |> 
      mutate(COMMUNE = .x, mode = NA)
    
    tcom <- bind_rows(
      tcom, 
      ttos |> anti_join(tcom, by=c("fromidINS", "toidINS"))) |> 
      arrange(COMMUNE, fromidINS, toidINS) |>
      mutate(euc = r3035::idINS2dist(fromidINS, toidINS)/1000)

    tcom.mean <- mean(tcom |> select(euc, t) |> filter(!is.na(t), t>0) |> mutate( v = euc / t) |> pull(v), na.rm=TRUE)

    tcom <- tcom |>
      mutate(
        euc = euc/tcom.mean,
        t = if_else(is.na(t)&euc<90, euc, t)) |> 
      select(-euc)
    
    tdir <- str_c(time_dts, "/", .x)
    rfn <- str_c(tdir, "/tt.parquet")
    dir.create(tdir)
    arrow::write_parquet(tcom, rfn)
    
  }, .progress=TRUE)
  
  time <- open_dataset(time_dts) |> 
    collect()
  
  arrow::write_parquet(time, "{dir_dist}/time.parquet" |> glue())
}
time <- arrow::read_parquet("{dir_dist}/time.parquet" |> glue(),
                            col_select = c(COMMUNE, fromidINS, toidINS, t)) |> 
  arrange(COMMUNE, fromidINS, toidINS)

t.vec <- time |> 
  pull(t) 
froms <- time |> distinct(fromidINS, COMMUNE) |> pull(fromidINS)
tos <- time |> distinct(toidINS) |> pull(toidINS)

rm(time)
gc()

tt <- matrix(t.vec, nrow = length(froms), 
             ncol = length(tos),
             byrow = TRUE) 

dimnames(tt) <- list(froms, tos)

tr <- matrixStats::rowRanks(tt, ties.method = "random")

qs::qsave(tt, time_matrix)
qs::qsave(tr, rank_matrix)
