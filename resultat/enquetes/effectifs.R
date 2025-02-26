#on essaye de reproduire les tableaux du SDES
library(tidyverse)
library(ofce)
library(conflicted)
library(sf)

conflict_prefer_all("dplyr", quiet=TRUE)

zones_epci <- bd_read("zones_res") |> 
  st_drop_geometry() |> 
  left_join(bd_read("communes") |>
              st_drop_geometry() |>
              select(INSEE_COM, SIREN_EPCI, COMMUNE, com, scot, POPULATION),
            by = c("CODE_COM"="INSEE_COM") ) |> 
  rename(zone_res = NUM_ZF_19) |> 
  select(zone_res, scot, CODE_COM)

emc2_individu <- bd_read("emc2_individu") |> 
  left_join(zones_epci , by = "zone_res" ) |> 
  transmute(
    scot,
    IDENT_IND = id_personne, 
    DENSITECOM_RES = class_densite_4,
    POND_JOUR = coeff_depl,
    pond_indC = coeff_depl,
    AGE = age,
    ACTOCCUP,
    across(starts_with("distance"), ~.x * 7),
    across(starts_with("nb_boucles"), ~.x * 7))

emc2_eff <- emc2_individu |> 
  group_by(!scot) |> 
  summarize(obs = n(), 
            ind = sum(pond_indC),
            adulte = sum(pond_indC[AGE>=18])) |> 
  mutate(across(where(is.numeric), ~.x + lag(.x, default = 0))) |> 
  mutate(src = "EMC^2^ AMP 2020",
         ligne = c("Métropole AMP", "Toute l'enquête"))

emp_eff <- bd_read("deploc_individu") |> 
  group_by(TAA2017_RES!=4) |> 
  summarize(obs = n(), 
            ind = sum(pond_indC),
            adulte = sum(pond_indC[AGE>=18])) |> 
  mutate(across(where(is.numeric), ~.x + lag(.x, default = 0))) |> 
  mutate(src = "EMP 2019 +700k",
         ligne = c("Agglo. de plus de 700k hors IdF", "France entière"))

effectifs <- bind_rows(
  emp_eff,
  emc2_eff ) |> 
  select(src, ligne, obs, ind, adulte)

return(effectifs = effectifs)
