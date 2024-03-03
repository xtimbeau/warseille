library(data.table, quietly = TRUE)
library(tidyverse, quietly = TRUE, warn.conflicts = FALSE)
library(stars, quietly = TRUE)
library(tmap, quietly = TRUE)
library(glue, quietly = TRUE)
library(qs, quietly = TRUE)
library(conflicted, quietly = TRUE)
library(rmeaps)
library(arrow)

conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("collect", "dplyr", quiet=TRUE)
conflict_prefer("between", "dplyr", quiet=TRUE)
conflict_prefer("first", "dplyr", quiet=TRUE)

arrow::set_cpu_count(8)

RhpcBLASctl::omp_set_num_threads(8)
RhpcBLASctl::blas_set_num_threads(8)

# ---- Definition des zones ----
cli::cli_alert_info("lecture de baselayer dans {.path {getwd()}}")
load("baselayer.rda")

source(newpredict_file)

transport <- c("walk", "bike", "transit", "car")

c200ze <- qs::qread(c200ze_file)
com_geo21_scot <- c200ze |> filter(scot) |> distinct(com21) |> pull(com21)
com_geo21_ze <- c200ze |> filter(emp>0) |> distinct(com21) |> pull(com21)

idINS2com21 <- c200ze |> pull(com21, name = idINS)

# ---- Fichier Mobilité professionnelle 2018 ----
# 
cli::cli_alert_info("lecture de {.path {enqmobpro}}")
mobpro <- fread(enqmobpro)

# Sélection Mobilité professionnelle 2018.
# Ceux qui habitent au sein de la SCOT d'interêt
filter_work <- map_lgl(as.integer(mobpro$DCLT), ~ any(.x == com_geo21_ze))
filter_live <- map_lgl(as.integer(mobpro$COMMUNE), ~ any(.x == com_geo21_scot))
mobpro <- mobpro[filter_work == TRUE & filter_live == TRUE,]

rm(filter_live, filter_work)
gc()

mobilites <- mobpro[, .(NB = sum(IPONDI)), by = c("COMMUNE", "DCLT", "NA5", "TRANS")]
mobilites <- mobilites[, .(NB = sum(NB, na.rm = TRUE)), by = c("COMMUNE", "DCLT", "TRANS")]
mobilites[, TRANS := factor(TRANS) |>
            fct_recode("none" = "1","walk" = "2", "bike" = "3", "car" = "4", "car" = "5", "transit" = "6")]
mobilites <- dcast(mobilites, COMMUNE + DCLT ~ TRANS, fun.aggregate = sum, value.var = "NB")
mobilites[, ':='(COMMUNE = factor(COMMUNE), DCLT = factor(DCLT))]

# ---- Ficher emplois estimés au carreau 200 ----
emp33km <- c200ze |> filter(emp>0) |> st_centroid()

lieux <- emp33km |> st_drop_geometry() |> select(toidINS = idINS, emp, DCLT = com21) |> setDT()
lieux[, emp_tot_dclt := sum(emp, na.rm=TRUE), by="DCLT"]

# ---- Sociodémographie au carreau 200 ----
## ---- Construction des départs avec sociodémographie ----

residents <- c200ze |> 
  filter(scot) |>
  transmute(
    fromidINS = idINS,
    ind, ind_18_64, tact1564, com21, ind_snv) |> 
  st_drop_geometry() |> 
  setDT()

residents[, act_tot_commune := sum(ind_18_64*tact1564, na.rm = TRUE), by = "com21"]

# ---- Travel Time Matrix ----

cli::cli_alert_info("lecture de {.path {distances_file}}")
distances <- arrow::read_parquet(distances_file, as_data_frame=FALSE) |>
  select(id, 
         travel_time_bike, travel_time_transit, 
         travel_time_car, travel_time_walk,
         distance_car, walktime) |> 
  collect() |> 
  as.data.frame() |> 
  setDT()

# On introduit le nombre de personnes en déplacement selon le mode de transport
cli::cli_alert_info("lecture de {.path {idINS_emp_file}}")
idINS <- arrow::read_parquet(idINS_emp_file, as_data_frame = FALSE) |> 
  select(id, fromidINS, toidINS, COMMUNE, DCLT, scot, is_emp=emp)  |> 
  collect() |> 
  as.data.frame() |> 
  setDT()

distances <- idINS[distances, on="id"]

distances <- mobilites[distances, on = c("COMMUNE", "DCLT")]
# 
# col_trans <- c("walk", "bike", "car", "transit")
# filter_nas <- distances[, lapply(.SD, is.na), .SDcol = col_trans] |> rowSums()
# distances <- distances[filter_nas != 4,]

# On introduit les emplois et la demo pour les carreaux from et to
residents <- residents[, .(fromidINS, ind_18_64, tact1564, act_tot_commune)]
lieux <- lieux[, .(toidINS, emp, emp_tot_dclt)]

distances <- merge(distances, residents, by = "fromidINS", all.x=TRUE)
distances <- merge(distances, lieux, by = "toidINS", all.x=TRUE)

# On ajoute des infos sur la densité des communes
# géographie 2021 -> join on COMMUNE/DCLT ok
cli::cli_alert_info("injection de densité ({.path {densitescommunes}})")
densite_urb <- readxl::read_xlsx("{densitescommunes}" |> glue(),
                                 skip = 4)
densite_urb <- densite_urb |>
  dplyr::select(-libgeo) |>
  mutate(across(.fns = factor)) |>
  as.data.table()

distances <- densite_urb[distances, on = c("codgeo"="COMMUNE") ] |>
  setnames(c("codgeo", "gridens"), c("COMMUNE", "DENSITECOM_ORI"))

distances <- densite_urb[distances, on = c("codgeo"="DCLT") ] |>
  setnames(c("codgeo", "gridens"), c("DCLT", "DENSITECOM_DES"))

# ATTENTION : ici on filtre sur l'emploi et les residences,
# ce qui réduit divise par 2 la taille de la matrice
distances <- distances[!is.na(emp), ]

# Calcul des proportions de population et d'emplois impliquées dans une liaison COMMUNE vers DCLT
# Attention : c'est plus tricky que de prendre la proportion pop au sein d'une commune ou
# la proportion d'emploi au sein d'une dclt. Mais cela permet de mieux coller aux informations de la
# "matrice" distances.
distances[, act :=  tact1564 * ind_18_64]

distances[, nb_from := .N, by = c("COMMUNE", "toidINS")]
distances[, nb_to := .N, by = c("DCLT", "fromidINS")]

distances[, total_act := sum(act / nb_to, na.rm = TRUE), by = c("COMMUNE", "DCLT")]
distances[, prop_act := act / total_act]

distances[, total_emp := sum(emp / nb_from, na.rm = TRUE), by = c("COMMUNE", "DCLT")]
distances[, prop_emp := emp / total_emp]

distances[, tout_mode := walk + bike + car + transit]
distances[, `:=`(walk=NULL, bike=NULL, car=NULL, transit=NULL)]
distances[is.na(tout_mode), tout_mode:= 0]

cli::cli_alert_success("écriture de {.path {temp_dir}}/distances_emplois.parquet")
arrow::write_parquet(distances, "{temp_dir}/distances_emplois.parquet"|> glue())
rm(distances)
gc()

mods <- load("{mod_rep}/MODpro.rda" |> glue())

# On reprend la matrice pour l'injection du modèle 
distances <- arrow::read_parquet("{temp_dir}/distances_emplois.parquet"|> glue())

tf_car <- 5
distances[, `:=`(
  DENSITECOM_DES = as.numeric(DENSITECOM_DES),
  DENSITECOM_ORI = as.numeric(DENSITECOM_ORI),
  densdes = factor(DENSITECOM_DES),
  densori = factor(DENSITECOM_ORI))]

distances[, `:=`(tt_car = travel_time_car,
                 travel_time_car = travel_time_car+tf_car)]

distances[, `:=`(delay_walk = travel_time_walk - travel_time_car,
                 delay_bike = travel_time_bike - travel_time_car,
                 delay_transit = travel_time_transit - travel_time_car,
                 delay_car = predict(MOD_congestion, newdata = distances),
                 delay_car_f = predict(MOD_congestion_f, newdata = distances))]

# on substitue walktime à time2stop
# de façon a être cohérent avec le calcul R5
# de façon à appliquer raisonnablement le modèle tc/notc
distances[, dist_tc_num := cut(walktime, 
                               breaks = c(0, 10, 15, 30, Inf),
                               include.lowest = TRUE,
                               labels = 1:4) |> as.numeric()]
distances[, tc := dist_tc_num != 4]

distances[ , delay_car := delay_car_f]

# On n'utilise pas l'info sur les distances différentes par mode.
# pris indirectement en compte par les delay
newdata_tc <- distances[ tc==TRUE ,
                         .(delay_walk, delay_bike, delay_transit, delay_car, 
                           dist_tc_num, MDISTTOT_fin = distance_car/1000, 
                           DENSITECOM_RES=factor(DENSITECOM_ORI), id)]
ids_tc <- newdata_tc$id
newdata_tc[, `:=`(motif = "motifs professionnels",
                  mode = "car",
                  POND_JOUR = 1)]

# Pour d'obscures raisons il faut rajouter une ligne des old data pour faire le predict
newdata_tc <- bind_rows(deploc_rum |> 
                          select(-dist_tc, -DENSITECOM_DES, -DENSITECOM_ORI, -dist_tc_so) |> 
                          mutate(POND_JOUR = 0) |> slice(1), 
                        newdata_tc) 
newdata_tc <- dfidx(as_tibble(newdata_tc), shape = "wide", choice = "mode", varying = 5:8, sep = "_") |>
  mutate(DENSITECOM_RES=factor(DENSITECOM_RES))

# on utilise la fonction newpredict (présente dans 4enq_mob_starter 
# car le predict a donné plusieurs problèmes 
# (toutefois, parfois il marche)
prediction_tc <- newpredict(MOD_rum_tc, newdata = newdata_tc)[-1,]
prediction_tc <- as.data.table(prediction_tc) |> 
  cbind(id = ids_tc)

# sans transit
newdata_notc <- distances[ tc==FALSE ,
                           .(delay_walk, delay_bike, delay_car, 
                             dist_tc_num, MDISTTOT_fin = distance_car/1000, id)]
ids_notc <- newdata_notc$id
newdata_notc[, `:=`(motif = "motifs professionnels",
                    mode = "car",
                    POND_JOUR = 1)]

# Pour d'obscures raisons il faut rajouter une ligne des old data pour faire le predict
newdata_notc <- deploc_rum |>
  filter(dist_tc_num == 4, mode != "transit") |> 
  select(-dist_tc, -dist_tc_num, -delay_transit,
         -DENSITECOM_DES, -DENSITECOM_ORI, -DENSITECOM_RES, 
         -dist_tc_so) |> 
  mutate(POND_JOUR = 0) |> 
  slice(1) |> 
  bind_rows(newdata_notc) 

newdata_notc <- dfidx(newdata_notc, shape = "wide", choice = "mode", varying = 4:6, sep = "_") |>
  mutate(delay2=delay^2)

prediction_notc <- newpredict(MOD_rum_notc, newdata = newdata_notc)[-1,]
prediction_notc <- as.data.table(prediction_notc) |> 
  cbind(id=ids_notc)
prediction_notc[, transit := 0]

predictions <- rbind(prediction_tc, prediction_notc)
setnames(predictions, transport, paste0("proba_", transport))

# Injection des proba de choix modal
distances <- predictions[distances, on = "id"]
# Calcul des effectifs (à partir des trajets fréquents capturés dans tout_mode)
distances[, (paste0(transport, "_pred")) := prop_act * prop_emp * tout_mode * .SD, 
          .SDcol = paste0("proba_", transport)]

cli::cli_alert_success("écriture de {.path {temp_dir}}/distances_emplois.parquet")
arrow::write_parquet(distances, "{temp_dir}/distances_emplois.parquet"|> glue())

rm(list=ls(all.names=TRUE))
gc(reset=TRUE)

# Nombre de trajets ------------------------------------------------------------

library(conflicted)
library(r3035)
library(data.table)
load("baselayer.rda")
cli::cli_alert_info("trajets")
data.table::setDTthreads(4)
arrow::set_cpu_count(4)

distances <- arrow::read_parquet("{temp_dir}/distances_emplois.parquet"|> glue())
setDT(distances)
trajets <- arrow::read_parquet(trajets_pro_file)
setDT(trajets)
# répartition des trajets à proportion des infos de mob pro
distances <- trajets[ distances , on = c("fromidINS", "toidINS")]
distances[, nbtrajets_par_ind_ij := trajet_pa]
rm(trajets)

# MEAPS intervient ici --------------------------------
meapsest <- qs::qread(meaps_file)
meapsstat <- qs::qread("output/meaps_stats.sqs")

algs_meaps <- meapsstat |>
  filter(!str_detect(alg, "gravitaire"))
algs_grav <- meapsstat |>
  filter(str_detect(alg, "gravitaire"))
source("estimation/f.normalisation.r")
source("annexes/meaps2.r")
Rcpp::sourceCpp("cpp/meaps_oddmatrix.cpp")

load(empetact_file)
N <- nrow(mat_distance)
K <- ncol(mat_distance)

walk(algs_meaps$alg, ~{
  gc()
  
  modds <- arrow::read_parquet("/scratch/estimations_meaps/{.x}.parquet" |> glue())
  rn <- modds |> pull(from)
  modds <- modds |> 
    select(-from) |> 
    as.matrix()
  rownames(modds) <- rn
  
  nb_tirages <- 256
  shufs <- matrix(NA, ncol = N, nrow = nb_tirages)
  for (i in 1:nb_tirages) shufs[i, ] <- sample(N, size = N)
  
  list_param <- list(
    rkdist = mat_rang_na, 
    emplois = les_emplois$emplois_reserves_c, 
    actifs = les_actifs$act_18_64c, 
    f = les_actifs$fuite, 
    shuf = shufs, 
    nthreads = 16, 
    modds = modds,
    progress=FALSE)
  
  meaps <- do.call(meaps_multishuf, list_param)
  dimnames(meaps) <- dimnames(mat_rang)
  meaps <- as.data.table(meaps, keep.rownames = TRUE)
  meaps <- melt(meaps, id.vars="rn")
  setnames(meaps, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
  meaps <- meaps[f_ij>0, ]
  dis <- merge(distances, meaps, by = c("fromidINS","toidINS"), all.x = TRUE)
  dis <- dis[f_ij>0, ]
  dis[ , actif_i := sum(f_ij, na.rm=TRUE), by= "fromidINS"]
  dis[, nbtrajets_par_ind_ij := trajet_pa * (f_ij / actif_i) ]
  
  dis <- dis[, .(
    id,
    tc,
    Kbcl,
    proba_bike,
    proba_car,
    proba_transit,
    proba_walk,
    f_ij,
    trajet_pa,
    nbtrajets_par_ind_ij)]
  
  nx <- str_replace_all(.x,"%","") |> 
    str_replace_all("é", "e") |> 
    str_replace_all(" ", "_")
  promeaps_file <- "{repository_distances}/proba_pro_meaps_{nx}.parquet" |> glue()
  cli::cli_alert_success("écriture de {.path {promeaps_file}}")
  arrow::write_parquet(dis, promeaps_file)
}, .progress = TRUE)

source("estimation/f.flux gravitaire.r")
pwalk(algs_grav, ~{
  gc()
  
  furness <- str_detect(..1, "avec furness")
  
  list_param <- list(
    dist = mat_distance_na, 
    emp = les_emplois$emplois_reserves_c, 
    hab = les_actifs$act_18_64c, 
    fuite = les_actifs$fuite, 
    delta = ..5,
    furness = furness)
  
  flux <- do.call(flux_grav, list_param)
  dimnames(flux) <- dimnames(mat_rang)
  flux <- as.data.table(flux, keep.rownames = TRUE)
  flux <- melt(flux, id.vars="rn")
  setnames(flux, c("rn","variable","value"), c("fromidINS", "toidINS", "f_ij"))
  flux <- flux[f_ij>0, ]
  dis <- merge(distances, flux, by = c("fromidINS","toidINS"), all.x = TRUE)
  dis <- dis[f_ij>0, ]
  dis[ , actif_i := sum(f_ij, na.rm=TRUE), by= "fromidINS"]
  dis[, nbtrajets_par_ind_ij := trajet_pa * (f_ij / actif_i) ]
  
  dis <- dis[, .(
    id,
    tc,
    Kbcl,
    proba_bike,
    proba_car,
    proba_transit,
    proba_walk,
    f_ij,
    trajet_pa,
    nbtrajets_par_ind_ij)]
  
  nx <- str_replace_all(.x,"%","") |> 
    str_replace_all("é", "e") |> 
    str_replace_all(" ", "_")
  promeaps_file <- "{repository_distances}/proba_pro_meaps_{nx}.parquet" |> glue()
  cli::cli_alert_success("écriture de {.path {promeaps_file}}")
  arrow::write_parquet(dis, promeaps_file)
}, .progress = TRUE)

t_toc <- toc(TRUE, TRUE)
timer <- tolower(lubridate::seconds_to_period(round(t_toc$toc-t_toc$tic)))

# if(nrow(probas[is.na(proba_car),])>0) { 
#   cli::cli_alert_danger(
#     "{nrow(probas)} paires d'o/d de probabilités avec {nrow(probas[is.na(proba_car),])} nas, en {timer}")
# } else {
#   cli::cli_alert_success(
#     "{nrow(probas)} paires d'o/d de probabilités pas de na, en {timer}")}


rm(list=ls(all.names=TRUE))
gc(reset=TRUE)
