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
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, 'SET temp_directory = "/tmp"')
DBI::dbExecute(con, 'SET max_temp_directory_size = "100GB"')
DBI::dbExecute(con, 'SET max_memory = "40GB"')
DBI::dbExecute(con, 'SET threads to 4')
DBI::dbGetQuery(con, "SELECT * FROM duckdb_settings();" )

duckdb2parquet <- function(data, output, by = NULL, overwrite = TRUE, options=NULL) {
  con <- dbplyr::remote_con(data)
  sql <- dbplyr::sql_render(data, con = con)
  if (is.null(options)) {
    options <- list()
  }
  if (!is.null(by)) {
    options <- c(list("PARTITION_BY" = str_c("(",str_c(by, collapse = ", "), ")")), options)
  }
  
  options <- lapply(options, toupper)
  options <- setNames(options, toupper(names(options)))
  options$FORMAT = 'PARQUET'
  
  parquet_options <- paste(paste0(names(options), " ", options), collapse = ", ")
  if(overwrite)
    parquet_options <- str_c(parquet_options, ", OVERWRITE")
  sql <- sprintf("COPY (%s) TO '%s' (%s)", sql, output, parquet_options)
  dbExecute(con, sql)
}

update_schema <- function(dataset, schema) {
  sch <- dataset$schema
  current <- map_chr(sch$fields, "name")
  new <- map_chr(schema$fields, "name")
  walk(new , ~ {sch[[.x]]<<-schema[[.x]]})
  open_dataset(fs::path_common(dataset$files), schema = sch)
}

# source("secrets/azure.R")
calc <- FALSE
check <- FALSE
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

# ---- Definition des zones ----
source("mglobals.r")

cli::cli_alert_info("Flux")

c200ze <- qs::qread(c200ze_file) |> arrange(com, idINS)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com) |> pull(com) |> as.integer()
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com) |> pull(com) |> as.integer()

froms <- open_dataset(time_dts) |> to_duckdb() |> 
  filter(COMMUNE %in% com_geo21_scot, DCLT %in% com_geo21_ze) |> 
  distinct(fromidINS) |> pull() |> as.character()
tos <- open_dataset(time_dts) |> to_duckdb() |> 
  filter(COMMUNE %in% com_geo21_scot, DCLT %in% com_geo21_ze) |> 
  distinct(toidINS) |> pull() |> as.character()

communes <- c200ze |> filter(scot) |> mutate(com = as.integer(com)) |> pull(com, name = idINS) 
communes <- communes[froms]
dclts <- c200ze |> filter(emp_resident>0) |> mutate(com = as.integer(com)) |> pull(com, name = idINS) 
dclts <- dclts[tos]

mobpro <- qs::qread(mobpro_file) |> filter(mobpro95) |> group_by(COMMUNE, DCLT) |> summarize(mobpro = sum(NB))
masses_AMP <- bd_read("AMP_masses")
actifs <- masses_AMP$actifs[froms]
emplois <- masses_AMP$emplois[tos]
fuite <- masses_AMP$fuites[froms]/actifs

COMs <- tibble(actifs = actifs, 
               fuite = fuite,
               from = froms,
               COMMUNE = communes )
DCLTs <- tibble(emplois = emplois, 
                to = tos, DCLT = dclts)

if(calc) {
  tranked <- qs::qread(trg_file)
  tic()
  meaps <- multishuf_oc(
    tranked, attraction="marche", parametres=c(13, 11.0455), nthreads = 8L)$flux
  meaps <- meaps |> 
    mutate(
      fromidINS = as.integer(fromidINS),
      toidINS = as.integer(toidINS)
    )
  toc()
  arrow::write_parquet(meaps, "{mdir}/meaps/meaps.parquet" |> glue())
  rm(tranked, meaps)
  gc()
} 

if(FALSE) {
  ## with_access
  library(MetricsWeighted)
  
  dists <- open_dataset(dist_dts) |> 
    filter(mode %in% c("car_dgr", "transit", "walk_tblr")) |>
    filter(travel_time <=30*60) |> 
    select(fromidINS, toidINS, distance, mode, tt=travel_time, COMMUNE, DCLT) 
  
  c200ze <- bd_read("c200ze") |> st_drop_geometry()
  
  dists <- dists |> 
    left_join(c200ze |> select(fromidINS = idINS, act_mobpro) |> filter(act_mobpro > 0), 
              by = "fromidINS") |> 
    left_join(c200ze |> select(toidINS = idINS, emp) |> filter(emp > 0), 
              by = "toidINS") 
  
  acc1 <- dists |> 
    filter(tt<=30*60) |> 
    group_by(mode, fromidINS) |> 
    to_duckdb() |> 
    summarize(emp = sum(emp, na.rm=TRUE), act = first(act_mobpro),
              .groups = "drop") |>
    collect() |> 
    pivot_wider(names_from = mode, values_from = emp) |> 
    mutate(
      transit = replace_na(transit, 0),
      walk_tblr = replace_na(walk_tblr, 0),
      rtc = (transit+1)/(car_dgr+1),
      rtw = (transit+1)/(walk_tblr+1),
      q10_tr = transit >= weighted_quantile(transit, act, 0.9),
      q10_rct = weighted_quantile(rtc, w = act, 0.9 )) 
 
  with_acces <- acc1 |> 
    filter(rtw>=2) 
}

if(FALSE) {
meaps <- arrow::open_dataset("{mdir}/meaps/meaps.parquet" |> glue()) |> 
  to_duckdb() |> 
  rename(f_ij = flux)

# meaps.c <- communaliser(meaps, communes, dclts)

# joining ----
delta <- arrow::open_dataset("/space_mounts/data/marseille/delta_iris") |> 
  to_duckdb() |> 
  mutate(
    nocar = bike+walk+transit,
    all = bike+walk+transit+car,
    trplus = transit+walk) |> 
  select(fromidINS, toidINS, car, nocar, trplus, all)

dists <- open_dataset(dist_dts) |> 
  to_duckdb() |> 
  filter(mode == "car_dgr") |> 
  select(fromidINS, toidINS, distance, COMMUNE) 

# times <- open_dataset(dist_dts) |>
#   select(fromidINS, toidINS, travel_time, mode) |> 
#   to_duckdb() |> 
#   filter(mode == "transit") |> 
#   select(fromidINS, toidINS, tt=travel_time) 

meaps.joined <- meaps |> 
  left_join(dists , by = c("fromidINS", "toidINS")) |> 
  compute()

meaps.joined <- meaps.joined |> 
  left_join(delta, by = c("fromidINS", "toidINS")) |> 
  compute()
}
# meaps.joined |> duckdb2parquet("test", by = "COMMUNE")
# meaps.joined |> duckdb2parquet("meaps.joined")

# meaps.part <- open_dataset("test") |> 
#   to_duckdb() 
# meaps.part2 <- open_dataset("test2") |> 
#   to_duckdb() 
# library(tictoc)
# 
# tic(); meaps.part |> 
#   group_by( COMMUNE, fromidINS) |> 
#   summarize(n()); toc()
# 
# tic(); read_parquet("test2") |>
#   to_duckdb() |> 
#   group_by( COMMUNE, fromidINS) |> 
#   summarize(n()); toc()
# 
# tic();meaps.joined |> 
#   group_by( fromidINS) |> 
#   summarize(n()) |> collect(); toc()


# meaps.joined <- meaps.joined |> 
#   left_join(times, by = c("fromidINS", "toidINS")) |> 
#   compute()

# mj1320 <- meaps.joined |> filter(COMMUNE %in% c(13201)) |> collect() 
# coms <- com_geo21_scot
# unlink("/space_mounts/data/marseille/mj_dts", recursive = TRUE, force = TRUE)
# dir.create("/space_mounts/data/marseille/mj_dts")
# 
# walk(coms, ~{
#   dir.create("/space_mounts/data/marseille/mj_dts/com={.x}" |> glue())
#   meaps.joined |> 
#     filter(COMMUNE==.x) |> 
#     collect() |> 
#     filter(!is.na(car)) |> 
#     write_parquet("/space_mounts/data/marseille/mj_dts/com={.x}/mj.parquet" |> glue()) },
#   .progress=TRUE)
# 
# }
# 
# ##
# meaps.joined <- open_dataset("/space_mounts/data/marseille/mj_dts") |> 
#   to_duckdb()
# gc()

meaps.joined <- open_dataset("meaps.joined") |> 
  to_duckdb()
tot <- meaps.joined |> summarize(f = sum(f_ij, na.rm=TRUE))

tr <- meaps.joined |> 
  group_by(fromidINS) |>
  summarize(
    fi = sum(f_ij, na.rm=TRUE),
    ki_transit = sum(f_ij * trplus/(distance+1), na.rm=TRUE),
    ki_nocar = sum(f_ij * nocar/(distance+1), na.rm=TRUE),
    ki_car = sum(f_ij * car/(distance+1), na.rm=TRUE),
    ki_all  = sum(f_ij * all/(distance+1), na.rm=TRUE) ) |> 
  mutate(across(starts_with("ki_"), ~.x/fi)) |> 
  mutate(
    pi_transit = ki_transit/ki_all,
    pi_nocar = ki_nocar/ki_all,
    pi_car = ki_car/ki_all) |> 
  ungroup() |> 
  filter(ki_all > 0) |> 
  collect() |> 
  left_join(acc1 |> select(fromidINS, transit,rtw, rtc)) |> 
  group_by(transit>20000) |> 
  summarize(
    f = sum(fi, na.rm=TRUE),
    nocar = sum(fi*pi_nocar, na.rm=TRUE),
    car = sum(fi*pi_car, na.rm=TRUE),
    transit = sum(fi*pi_transit, na.rm=TRUE)) |> 
  mutate(across(c(nocar, car, transit), ~.x/f)) |> 
  collect()
tr
