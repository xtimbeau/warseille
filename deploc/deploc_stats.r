#on essaye de reproduire les tableaux du SDES
library(tidyverse)
library(ofce)
library(conflicted)
library(here)
library(knitr)
source("mglobals.r")
conflict_prefer_all("dplyr", quiet=TRUE)
future::plan("multisession", workers = 4)

stat_bouc <- source_data("deploc/deploc.r")
deploc_individu <- bd_read("deploc_individu")
source("deploc/f.deploc_stat.r")

aa4 <- deploc_densite_2(
  deploc_individu |> filter(TAA2017_RES==4), label_reg="Aire attraction plus de 700k (hors Paris)",
  var = "DENSITECOM_RES", 
  mode = "car",
  soustitre = ", en voiture",
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"))
EMP2019_AA4 <- aa4$raw
bd_write(EMP2019_AA4)
aa4$table$km

# table pour EMC2

## périmètre de l'epci uniquement
library(sf)
zones_epci <- bd_read("zones_res") |> 
  st_drop_geometry() |> 
  left_join(bd_read("communes") |>
              st_drop_geometry() |>
              select(INSEE_COM, SIREN_EPCI, COMMUNE, com, scot, POPULATION),
            by = c("CODE_COM"="INSEE_COM") ) |> 
  rename(zone_res = NUM_ZF_19)

emc2_individu <- bd_read("emc2_individu") |> 
  semi_join(zones_epci |> filter(scot), by = "zone_res" ) |> # epci seulement
  transmute(
    IDENT_IND = id_personne, 
    DENSITECOM_RES = class_densite_4,
    POND_JOUR = coeff_depl,
    pond_indC = coeff_depl,
    AGE = age,
    ACTOCCUP,
    across(starts_with("distance"), ~.x * 7),
    across(starts_with("nb_boucles"), ~.x * 7))

emc4 <- deploc_densite_2(
  emc2_individu, K=512,
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"),
  titre = "Métropole d'Aix-Marseille-Provence",
  source = "EMC^2^ AMP 2019")
emc4$table$km

emc4_car <- deploc_densite_2(
  emc2_individu, K=512, mode ="car",
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"),
  titre = "Métropole d'Aix-Marseille-Provence",
  soustitre = ", en voiture",
  source = "EMC^2^ AMP 2019")
emc4_car$table$km

EMC2_AMP <- emc4_car$raw
bd_write(EMC2_AMP)
emc4_car$table$km
