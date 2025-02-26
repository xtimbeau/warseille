# On part des enquêtes mobilités, on le sresample pour calculer les IC à 95%
library(tidyverse)
library(ofce)

source("f.deploc_stat.r")

emp_individu <- bd_read("deploc_individu")

future::plan("multisession", workers = 8)
# future::plan("sequential")

emp_tte <- deploc_densite_2(
  emp_individu, label_reg="France entière",
  var = "DENSITECOM_RES", 
  mode = "car",
  soustitre = ", en voiture",
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"))

emp_4 <- deploc_densite_2(
  emp_individu |> filter(TAA2017_RES==4), label_reg="Aire attraction plus de 700k (hors Paris)",
  var = "DENSITECOM_RES", 
  mode = "car",
  soustitre = ", en voiture",
  labels = c("très dense", "densité intermédiaire", "peu dense", "très peu dense"))


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
    class_densite_7,
    POND_JOUR = coeff_depl,
    pond_indC = coeff_depl,
    AGE = age,
    ACTOCCUP,
    across(starts_with("distance"), ~.x * 7),
    across(starts_with("nb_boucles"), ~.x * 7))

emc2_4 <- deploc_densite_2(
  emc2_individu |> filter(DENSITECOM_RES!=4), K=512,
  labels = c("très dense", "densité intermédiaire", "peu dense"),
  titre = "Métropole d'Aix-Marseille-Provence",
  source = "EMC^2^ AMP 2019")

emc2_4_car <- deploc_densite_2(
  emc2_individu |> filter(DENSITECOM_RES!=4), K=512, mode ="car",
  labels = c("très dense", "densité intermédiaire", "peu dense"),
  titre = "Métropole d'Aix-Marseille-Provence",
  soustitre = ", en voiture",
  source = "EMC^2^ AMP 2019")

emc2_7_car <- deploc_densite_2(
  emc2_individu |> filter(class_densite_7 != 7), K=512, mode ="car",
  titre = "Métropole d'Aix-Marseille-Provence",
  var = "class_densite_7",
  soustitre = ", en voiture",
  source = "EMC^2^ AMP 2019")

return(list(emc2_4_car = emc2_4_car, emc2_7_car = emc2_7_car, emp_4 = emp_4, emp_tte = emp_tte, 
            emp = emp_individu, emc2 = emc2_individu))
