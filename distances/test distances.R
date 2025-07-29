setwd("~/marseille")
library(tidyverse, quietly = TRUE, warn.conflicts = FALSE)
library(glue, quietly = TRUE, warn.conflicts = FALSE)
library(arrow, quietly = TRUE, warn.conflicts = FALSE)
load("baselayer.rda")
dist <- open_dataset(dist_dts) |> 
  to_duckdb() |> 
  filter(travel_time>0)
mobpro <- qs::qread(mobpro_file) |> 
  group_by(COMMUNE, DCLT) |> 
  summarize(flux = sum(NB),
            mobprob95 = any(mobpro95),
            n_mobpro = sum(NB>0), .groups = "drop") 

cist <- dist |>  group_by(COMMUNE, DCLT) |>
  filter(travel_time>0) |> 
  summarize(n = n(), .groups = "drop") |> 
  collect() |> 
  full_join(mobpro)

cist |> filter(mobprob95) |> count(is.na(n_mobpro))
cist |> filter(mobprob95) |> count(is.na(n))

cist |> filter(mobprob95) |> filter(is.na(n))

vist <- open_dataset("/space_mounts/data/marseille/distances/src/car_dgr") |> 
  to_duckdb() |> 
  filter(travel_time>0) |>
  mutate(COMMUNE = as.character(COMMUNE)) |> 
  group_by(COMMUNE, DCLT) |> 
  summarise(n = n(), .groups = "drop") |> 
  collect() |> 
  full_join(mobpro)

vist |> filter(is.na(n), mobprob95)
vist |> filter(mobprob95)|> count(is.na(n_mobpro))

COMMUNEs <- mobpro |> filter(mobprob95) |> distinct(COMMUNE) |> pull()
DCLTs <- mobpro |> filter(mobprob95) |> distinct(DCLT) |> pull()

idINSes <- qs::qread(c200ze_file) |> 
  st_drop_geometry() |> 
  select(com=com, idINS, scot, emp_resident, ind) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com%in%DCLTs) |> 
  filter(from | to)

idINSes |>  filter(to) |>  distinct(com)
