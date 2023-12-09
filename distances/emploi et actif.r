# à partir de mobpro principalement ce programme calcule les emplois et les actifs et la fuite
# nécessaires pour le calcul de MEAPS
# 
# en sortie : les_emplois, les_actifs, mat_rang

library(tidyverse)
library(glue)
library(sf)
library(data.table)
library(matrixStats)
library(here)
load("baselayer.rda")
conflicted::conflict_prefer_all("dplyr", quiet = TRUE)
marges <- load("marges_mobpro.rda")

## ---- 1. CALCUL DES MARGES ACTIFS ET EMPLOIS ----
# NB : les emplois sont restreints à ceux pris par ces actifs-là.
# NB : pour les actifs, une fuite par commune est évaluée.
les_mobiles <- les_mobiles |> 
  mutate(
    stat2 = if_else(
      statut == "sal_prive", 
      "visible", 
      "invisible")) |> 
  group_by(DCLT, stat2) |>
  summarise(Out = sum(Out),
            In = sum(In)) |> 
  pivot_wider(
    names_from = stat2,
    values_from = c(Out, In),
    values_fill = 0) |> 
  mutate(
    ratio_toutemplois =
      (Out_visible + Out_invisible + In_visible + In_invisible)/
      (In_visible + Out_visible),
    ratio_In =
      (In_visible + In_invisible)/
      (Out_visible + Out_invisible + In_visible + In_invisible))

c200 <- qs::qread(c200ze_file)
# on récupère les emplois atteints au moins une fois
# peut être plus d'une fois ?
distance <- arrow::read_parquet(
  distances_file,
  col_select = c("id",
                 "travel_time_bike", "travel_time_car",
                 "travel_time_transit", "travel_time_walk",
                 "distance_car"))

idINS <- arrow::read_parquet(
  idINS_emp_file, 
  col_select = c("id", "fromidINS", "toidINS", "emp", "COMMUNE", "DCLT"))
setDT(idINS)
c200ze <- qs::qread(c200ze_file) 
setDT(c200ze)

idINS <- merge(idINS, c200ze[ind>0,.(fromidINS =idINS, ind)], by="fromidINS", all.x=TRUE)
idINS <- merge(idINS, c200ze[emp>0,.(toidINS =idINS, emplois = emp)], by="toidINS", all.x=TRUE)

distance <- merge(distance, by="id", all.x=TRUE)
distance <- distance[emp==TRUE,]

distance[, w:=emplois*ind]
distance[is.na(w), w := 0]
distance[, distance_car :=  as.numeric(distance_car)]
library(MetricsWeighted)
dist.com <- distance[
  ,  
  tt_min := pmin(
    travel_time_bike,
    travel_time_car+5, 
    travel_time_transit,
    travel_time_walk, 
    na.rm = TRUE)][
      , .(
        d = weighted_median(distance_car, w = w, na.rm=TRUE),
        d5 = weighted_quantile(distance_car, w = w, probs = 0.05, na.rm=TRUE),
        d95 = weighted_quantile(distance_car, w = w, probs = 0.95, na.rm=TRUE),
        t = weighted_median(tt_min, w = w, na.rm=TRUE),
        t5 = weighted_quantile(tt_min, w = w, probs = 0.05, na.rm=TRUE),
        t95 = weighted_quantile(tt_min, w = w, probs = 0.95, na.rm=TRUE)),
      by=c("COMMUNE", "DCLT")] [!is.na(d)|!is.na(t),]
setorder(dist.com, COMMUNE, DCLT)

save(dist.com, file= "output/dist.com.srda")

distance[, `:=`(COMMUNE=NULL, DCLT=NULL, w=NULL, ind = NULL, emplois = NULL)]

toidINS <- unique(distance$toidINS)
# on filtre en ne gardant que les emplois accessibles
# tels que définit par la matrice de distance
les_emplois <- st_drop_geometry(c200) |> 
  filter(emp > 0) |> 
  select(idINS, emplois = emp) |> 
  filter(idINS %in% toidINS)

popact <- readxl::read_xlsx(
  "{popactive_file}" |> glue(), sheet = "COM_2018", skip=5) |> 
  transmute(com=CODGEO,
            libcom = LIBGEO,
            pop1564 = P18_POP1564,
            act1564 = P18_ACT1564)

# RQ : la géo de MOBPRO2018 est celle de 2021.
les_actifs <- c200 |>
  filter(scot, ind_18_64>0) |> 
  st_drop_geometry() |> 
  select(idINS, com=com21, ind_18_64) |> 
  as_tibble()

## ---- 2. CALCUL DE LA MATRICE DE RANG DES TEMPS DE TRAJETS ----
distance <- arrow::read_parquet(
  distances_file,
  col_select = c("id", 
                 "travel_time_bike", "travel_time_car",
                 "travel_time_transit", "travel_time_walk",
                 "distance_car"))

idINS <- arrow::open_dataset(idINS_emp_file) |> 
  select(id, toidINS, fromidINS, COMMUNE, DCLT) |> 
  filter(toidINS %in% les_emplois$idINS) |> 
  collect() |> 
  as.data.frame() |> 
  setDT()

distance <- distance[idINS, on = "id"]
#distance <- probas[distance, on = "id"]

rm(idINS)

# AJOUT de 5 mn à la voiture
distance[, tt_min := pmin(
  travel_time_bike,
  travel_time_car+5, 
  travel_time_transit,
  travel_time_walk, 
  na.rm = TRUE)]

mat_distance <- distance[, .(fromidINS, toidINS, tt_min)]
mat_distance <- dcast(
  mat_distance, 
  fromidINS ~ toidINS, 
  value.var = "tt_min") |> 
  as.matrix(rownames = "fromidINS")

# NB : en cas de temps égaux, le rang est tiré au hasard
mat_rang <- rowRanks(mat_distance, ties.method = "random")
dimnames(mat_rang) <- dimnames(mat_distance)

N = nrow(mat_distance)
K = ncol(mat_distance)

## ---- 3. SUITE CALCUL DES MARGES ----
# NB : on ordonne les marges sur l'ordre de la matrice de rang.
# NB : on considère comme "fuyants" aussi les immobiles.
les_actifs <- left_join(
  tibble(idINS=dimnames(mat_rang)[[1]]),
  les_actifs , 
  by = c("idINS")
) |> 
  left_join(popact, by="com") |> 
  mutate(act_18_64 = ind_18_64 * act1564/pop1564)

les_emplois <- left_join(
  tibble(idINS=dimnames(mat_rang)[[2]]), 
  les_emplois,
  by = "idINS")

les_emplois <- les_emplois |> 
  left_join(
    distance[, .(toidINS, DCLT)] |> unique(),
    by = c("idINS" = "toidINS")) |> 
  mutate(DCLT = as.character(DCLT)) |> 
  left_join(les_mobiles |> select(DCLT, ratio_In) , by = "DCLT") |> 
  mutate(ratio_In = replace(ratio_In, is.na(ratio_In), 0))

# La mobilité selon MOBPRO 
mobpro <- mobilites[
  live_in == TRUE & work_in == TRUE,
][,
  .(mobpro = sum(NB)), by = c("COMMUNE", "DCLT")]

#taux_fuite_moyen <-
# 1 - sum(les_emplois$emplois_rat) / sum(les_actifs$ind_18_64)
les_actifs <- left_join(
  les_actifs |> select(idINS, COMMUNE = com, act_18_64), 
  les_fuites |> select(COMMUNE, fuite), 
  by = "COMMUNE") |> 
  # dans quelques communes la fuite est nulle, 
  # ce qui pose un problème ensuite
  # meaps calcule un flux nul dans ce cas...
  # on met un peu de fuite 
  mutate(
    fuite = ifelse(fuite == 0, min(fuite[fuite!=0]), fuite))

les_actifs <- left_join(
  les_actifs,
  mobpro[, .(tot_actifs_mobpro = sum(mobpro)), by = "COMMUNE"], 
  by = "COMMUNE")

# IN FINE, on cale sur mobpro.
# le petit c indique le calage sur mobpro pour act_18_64c et emplois_reserves_c
les_actifs <- les_actifs |> 
  group_by(COMMUNE) |> 
  mutate(tot_actifs_inzone = sum(act_18_64 * (1 - fuite)),
         correction = tot_actifs_mobpro / tot_actifs_inzone,
         act_18_64c = correction * act_18_64) |> 
  ungroup()

les_emplois <- left_join(
  les_emplois,
  mobpro[, .(tot_emplois_mobpro = sum(mobpro)), by = "DCLT"],
  by = "DCLT"
) |> 
  mutate(tot_emplois_mobpro = replace(tot_emplois_mobpro, 
                                      is.na(tot_emplois_mobpro), 0))

les_emplois <- les_emplois |>
  group_by(DCLT) |> 
  mutate(emplois_reserves = emplois * ratio_In,
         tot_emplois_reserves = sum(emplois_reserves),
         correction = ifelse(tot_emplois_reserves != 0,
                             tot_emplois_mobpro / tot_emplois_reserves,
                             0),
         emplois_reserves_c = emplois_reserves * correction) |> 
  ungroup(DCLT)

# on fabrique la matrice NAsifié
# 
source("estimation/f.normalisation.r")
mat_distance_na <- NAsifie_distance(mat_distance, les_actifs, les_emplois, mobpro)
mat_rang_na <- rowRanks(mat_distance_na, ties.method = "random")
dimnames(mat_rang_na) <- dimnames(mat_distance_na)
gc()

save(les_actifs, les_emplois, mat_distance,
     mat_rang, mat_distance_na, mat_rang_na, mobpro, 
     file = empetact_file)