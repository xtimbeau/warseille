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
library(tictoc)
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
DBI::dbExecute(con, 'PRAGMA temp_directory = "/tmp"')
DBI::dbExecute(con, 'PRAGMA max_temp_directory_size = "100GB"')
DBI::dbExecute(con, 'PRAGMA memory_limit = "16GB"')
DBI::dbExecute(con, 'SET threads to 8')
DBI::dbExecute(con, 'PRAGMA preserve_insertion_order=false')

# ---- Definition des zones ----
source("mglobals.r")

compute_scn <- function(data) {
  raw <- data |> 
    summarise(
      f_id = sum(f_ij),
      car = sum(f_ij*car), 
      bike = sum(f_ij*bike),
      walk = sum(f_ij*walk),
      transit = sum(f_ij*transit),
      all = sum(f_ij*all),
      .by = c(fromidINS, d)) |> 
    right_join(c200ze |> select(idINS, densite), 
               join_by(fromidINS == idINS), copy = TRUE) |> 
    collect()
  
  ddensite <- raw |>
    summarise(
      f = sum(f_id, na.rm=TRUE),
      car = sum(car), 
      bike = sum(bike),
      walk = sum(walk),
      transit = sum(transit),
      all = sum(all),
      .by = c(d, densite)) |> 
    mutate(
      across(c(car, bike, walk, transit), ~.x/all) ) |> 
    collect() |> 
    pivot_longer(c(car, bike, walk, transit)) 
  
  c200 <- raw |>
    summarise(
      f_i = sum(f_id, na.rm=TRUE),
      car = sum(car), 
      bike = sum(bike),
      walk = sum(walk),
      transit = sum(transit),
      all = sum(all),
      .by = c(fromidINS)) |>
    left_join(c200ze |> select(idINS, ind) |> collect(), join_by(fromidINS == idINS)) |> 
    filter(ind>0) |> 
    mutate(
      across(c(car, bike, walk, transit), ~.x/all) ) 
  return(list(dd = ddensite, c200 = c200))
}
co2km <- 218/1e+6
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
c200ze |> collect() |> bd_write("c200ze_densite")

ind.densite <- c200ze |> 
  summarize(ind = sum(ind, na.rm=TRUE),
            .by = densite) |> 
  collect()
coms <- c200ze |> distinct(com) |> pull(com)

if(!dir.exists("/tmp/joined")) {
  if(!dir.exists("/tmp/dists")) {
    unlink("/tmp/dists", recursive=TRUE, force = TRUE)
    fs::dir_copy(dist_dts, "/tmp/dists", overwrite = TRUE)
  }
  if(!dir.exists("/tmp/meaps")) {
    unlink("/tmp/meaps", recursive=TRUE, force = TRUE)
    fs::dir_copy(meaps.joined_file, "/tmp/meaps", overwrite = TRUE)
  }
  unlink("/tmp/joined", recursive = TRUE)
  tic()
  c200ze <- qs::qread(c200ze_file) |> 
    st_drop_geometry() |>
    select(idINS, ind, com, scot) |> 
    left_join(communes, join_by(com)) |> 
    filter(scot) |> 
    to_duckdb(con = con)
  
  tbl1 <- parquet2duckdb(
    con,  "trtage",
    parquet = "/tmp/dists/**/*.parquet",
    hive = "true")
  
  transitage <- tbl(con, tbl1) |>
    filter(mode %in% c("transit", "car_dgr")) |> 
    select(fromidINS, toidINS, tt = travel_time, at = access_time, mode ) |> 
    semi_join(c200ze, join_by(fromidINS == idINS)) |> 
    pivot_wider(names_from = mode, values_from = c(tt, at)) |> 
    select(-at_car_dgr, at = at_transit) |> 
    mutate(
      at = at/60,
      transit = if_else(is.na(tt_transit), 120, tt_transit/60),
      car = tt_car_dgr/60,
      dt = (tt_transit - tt_car_dgr)/60,
      rdt = tt_transit / tt_car_dgr) |>
    transmute(
      fromidINS, toidINS,
      transitage = (at<=10) & (transit <= 45) & (dt <= 15 | rdt <= 1.25) ) 
  
  tbl <- parquet2duckdb(
    con,
    table = "meaps",
    parquet = "/tmp/meaps/**/*.parquet",
    hive = "true")  
  
  tbl(con, tbl) |>
    mutate(d = round(distance/1000,1)) |>
    filter(d<=60, d >  0) |> 
    left_join(transitage, join_by(fromidINS, toidINS)) |> 
    duckdb2parquet("/tmp/joined", by = "COMMUNE")
  toc()
}
gc()

meaps.joined <- open_dataset("/tmp/joined") |> 
  to_duckdb()
pm.ref <- meaps.joined |>
  compute_scn()

pm.ref$c200 |> 
  transmute(idINS = r3035::expand_idINS(fromidINS),
         actif  = f_i, ind,
         car, bike, walk, transit) |> 
  bd_write("partmodales_c200")

pm.transit <- meaps.joined |>
  mutate(dtransit = 0.45*transit*if_else(transitage, 1, 0)) |>
  mutate(dtransit = if_else(dtransit < car, dtransit, car)) |> 
  mutate(car = car - dtransit,
         transit = transit + dtransit) |> 
  compute_scn()

pm.velo <- meaps.joined |>
  mutate(d = round(distance/1000,1)) |>
  filter(d<=60, d > 0) |>
  mutate(dbike = 1*bike*if_else(d < 10, 1, if_else(d < 2*10, -(d-2*10)/10, 0))) |>
  mutate(dbike = if_else(dbike < car, dbike, car)) |> 
  mutate(car = car - dbike,
         bike = bike + dbike) |> 
  compute_scn()

pm <- bind_rows(
  pm.ref$dd |> mutate(s = "ref"),
  pm.transit$dd |> mutate(s = "transit"),
  pm.velo$dd |> mutate(s = "velo") ) |> 
  mutate(
    f = replace_na(f, 0),
    all = replace_na(all, 0),
    value = replace_na(value, 0))

pmr <- pm |> 
  pivot_wider(names_from = name, values_from = value) |> 
  group_by(densite, s) |> 
  summarize(
    co2 = sum(car*all) * co2km,
    f = sum(f),
    across(c(car, bike, walk, transit), ~sum(.x*all, na.rm=TRUE) / sum(all, na.rm=TRUE)),
    all = sum(all),
    .groups = "drop") |>
  left_join(ind.densite, join_by(densite)) |> 
  mutate(co2_pi = co2/ind) |> 
  mutate(densite = fct_reorder(densite, co2_pi) )

pmr <- pmr |> 
  bind_rows(
    pmr |> 
      group_by(s) |> 
      summarize(
        across(c(car, bike, walk, transit), ~sum(.x*all, na.rm=TRUE)/sum(all, na.rm=TRUE)),
        across(c(co2, ind, f, all), ~sum(.x, na.rm=TRUE)) ,
        densite = factor("total"),
        .groups = "drop") ) |> 
  mutate(
    co2_pi = co2/ind  ) |> 
  relocate(s, densite, ind, f, co2, co2_pi, car, bike, walk, transit)

bd_write(pmr, "partsmodales")

# détail au c200 des parts modales

pm.c200 <- bind_rows(
  pm.ref$c200 |> mutate(s = "ref"),
  pm.transit$c200 |> mutate(s = "transit"),
  pm.velo$c200 |> mutate(s = "velo") ) |> 
  mutate(idINS = r3035::expand_idINS(fromidINS)) |> 
  relocate(idINS) |> 
  relocate(fromidINS, .after = -1)
  
bd_write(pm.c200, "partsmodales.c200")

# ggplot(pmd) + stat_ecdf(aes(x=d, weight = f, color = s, group = s), geom = "step")
# 
# ggplot(pm ) + 
#   geom_col(aes(x=d, y = value, fill=name), width = 1, color=NA, position = "stack") + 
#   facet_grid(rows = vars(densite), cols = vars(s))
# 
# ggplot(pm |> filter(d<=50, d>0, name == "bike")) + 
#   # geom_col(aes(x=d, y = value, fill=name), width = 1, alpha = 0.5, color=NA, position = "stack")+
#   geom_col(aes(x=d, y = value*(1+ 3/10  ) -value), fill="blue", alpha = 1, width = 1, color=NA) +
#   geom_col(aes(x=d, y = value*(1+ 3/10 * (value/max(value)) ) -value), fill="green", alpha = 1, width = 1, color=NA)

pmr |> 
  group_by(densite) |> 
  summarize(
    dlogco2 =  co2[s=="transit"]/co2[s=="ref"]-1,
    across(c(co2, co2_pi, car, bike, walk, transit), ~.x[s=="transit"]-.x[s=="ref"]) ) |> 
  relocate(densite, co2, dlogco2, co2_pi, car, bike, walk, transit) |> 
  arrange(densite) |> 
  gt() |>
  fmt_number(co2, scale = 1/1000, decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  fmt_number(co2_pi, decimals = 3, sep_mark = " ", dec_mark = ",") |> 
  tab_style(cell_text(weight = "bold"),
            cells_body(row = densite == "total" )) |>
  fmt_percent(c(car, bike, walk, transit), decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  fmt_percent(dlogco2, decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  cols_align("left", densite) |> 
  tab_spanner(md("variation du CO<sub>2</sub>"), c(co2, dlogco2, co2_pi)) |>
  tab_spanner(md("variation de la part modale"), columns = c(car, bike, walk, transit)) |> 
  cols_label(
    densite = "",
    car = "voiture",
    bike = "vélo",
    walk = "marche",
    transit = "T.C.",
    co2 = "niveau",
    dlogco2 = "relative",
    co2_pi = "par individu") |> 
  tab_footnote(md("milliers de tonnes de CO2"), locations = cells_column_labels(co2)) |>
  tab_footnote(md("tonne de CO2 par personne"), locations = cells_column_labels(co2_pi))|>
  tab_footnote(md("part modale en km parcourus"), locations = cells_column_labels(c(car, bike, walk, transit))) |> 
  tab_header("Transit")

pmr |> 
  group_by(densite) |> 
  summarize(
    dlogco2 =  co2[s=="velo"]/co2[s=="ref"]-1,
    across(c(co2, co2_pi, car, bike, walk, transit), ~.x[s=="velo"]-.x[s=="ref"]) ) |> 
  relocate(densite, co2, dlogco2, co2_pi, car, bike, walk, transit) |> 
  arrange(densite) |> 
  gt() |>
  fmt_number(co2, scale = 1/1000, decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  fmt_number(co2_pi, decimals = 3, sep_mark = " ", dec_mark = ",") |> 
  tab_style(cell_text(weight = "bold"),
            cells_body(row = densite == "total" )) |>
  fmt_percent(c(car, bike, walk, transit), decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  fmt_percent(dlogco2, decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  cols_align("left", densite) |> 
  tab_spanner(md("variation du CO<sub>2</sub>"), c(co2, dlogco2, co2_pi)) |>
  tab_spanner(md("variation de la part modale"), columns = c(car, bike, walk, transit)) |> 
  cols_label(
    densite = "",
    car = "voiture",
    bike = "vélo",
    walk = "marche",
    transit = "T.C.",
    co2 = "niveau",
    dlogco2 = "relative",
    co2_pi = "par individu") |> 
  tab_footnote(md("milliers de tonnes de CO2"), locations = cells_column_labels(co2)) |>
  tab_footnote(md("tonne de CO2 par personne"), locations = cells_column_labels(co2_pi))|>
  tab_footnote(md("part modale en km parcourus"), locations = cells_column_labels(c(car, bike, walk, transit))) |> 
  tab_header("Vélo")

pmr |> 
  filter(s=="ref", ind > 1000) |> 
  select(densite, ind, co2, co2_pi, car, bike, walk, transit) |> 
  arrange(densite) |> 
  gt() |>
  cols_align("left", densite) |> 
  fmt_number(c( ind), scale = 1/1000, decimals = 0, sep_mark = " ", dec_mark = ",") |> 
  fmt_number(c(co2), scale = 1/1000, decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  fmt_number(co2_pi, decimals = 2, sep_mark = " ", dec_mark = ",") |>  
  fmt_percent(c(car, bike, walk, transit), decimals = 1, sep_mark = " ", dec_mark = ",") |> 
  tab_style(cell_text(weight = "bold"),
            cells_body(row = densite == "total" )) |>
  cols_label(
    densite = "",
    ind = "Population",
    car = "voiture",
    bike = "vélo",
    walk = "marche",
    transit = "T.C.",
    co2 = md("CO<sub>2</sub>"),
    co2_pi = md("CO<sub>2</sub>/individu")) |> 
  tab_footnote("milliers de personnes", locations = cells_column_labels(ind)) |>
  tab_footnote(md("milliers de tonnes de CO2"), locations = cells_column_labels(co2)) |>
  tab_footnote(md("tonne de CO2 par personne"), locations = cells_column_labels(co2_pi))|>
  tab_footnote(md("part modale en km parcourus"), locations = cells_column_labels(c(car, bike, walk, transit)))


dbike.c200 <- pm.ref$c200 |> 
  left_join(pm.velo$c200, join_by(fromidINS), suffix = c(".1", ".2")) |>
  mutate(bike = bike.2 - bike.1, 
         car = car.2-car.1, 
         transit = transit.2 - transit.1,
         walk = walk.2 - walk.1) |>
  filter(f_i.1>0) |>
  select(fromidINS, car, bike, transit, walk)
dtransit.c200 <- pm.ref$c200 |> 
  left_join(pm.transit$c200, join_by(fromidINS), suffix = c(".1", ".2")) |>
  mutate(bike = bike.2 - bike.1,
         car = car.2-car.1, 
         transit = transit.2 - transit.1,
         walk = walk.2 - walk.1) |>
  filter(f_i.1>0) |>
  select(fromidINS, car, bike, transit, walk)

bd_write(dbike.c200)
bd_write(dtransit.c200)
