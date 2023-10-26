#ce code suive "format_datas" dans le processus de production de distances_emplois
source("~/marseille/enq_mobilite_starter.r")
library(mlogit)
library(data.table)

transport <- c("walk", "bike", "transit", "car")

# Sélection des trajets locaux et unimodaux
deploc <- deploc |> 
  filter(mobloc == 1, unimodal) |> 
  dplyr::mutate(motif_pro = ifelse(dplyr::between(MMOTIFDES, 9.0, 9.9), "motifs professionnels", "autres motifs"),
         dist_tc = ordered(dist_tc, levels = c("Moins de 300m", "301 à 600m", "601 à 999m", "Plus de 1km")),
         dist_tc_num = as.numeric(dist_tc))

MOD_tt <- lm(DUREE ~ MDISTTOT_fin*mode, data = deploc)

deploc <- bind_cols(deploc, 
                    map_dfc(transport, ~ tibble("tt_{.x}" := predict(MOD_tt, newdata = data.frame(MDISTTOT_fin = deploc$MDISTTOT_fin, 
                                                                                                  mode = .x)))))
# Construction de la base pour le modèle de choix modal (Random Utility Model)
deploc_rum <- deploc |>
  dplyr::mutate(delay_walk = tt_walk - tt_car,
         delay_bike = tt_bike - tt_car,
         delay_transit = tt_transit - tt_car,
         delay_car = DUREE - tt_car) |> 
  select(mode, dist_tc, dist_tc_num, motif_pro, POND_JOUR, starts_with("delay_"), MDISTTOT_fin, DENSITECOM_ORI,DENSITECOM_DES, DENSITECOM_RES) |> 
  drop_na() |> 
  dplyr::mutate(DENSITECOM_ORI = factor(DENSITECOM_ORI),
         DENSITECOM_RES = factor(DENSITECOM_RES),
         DENSITECOM_DES = factor(DENSITECOM_DES),
         dist_tc_so = factor(dist_tc_num),
         dist_tc_num2 = dist_tc_num^2)

# mobi_rum <- dfidx(deploc_rum, shape = "wide", choice = "mode", varying = 5:8, sep = "_")
# 
# MOD_rum <- mlogit(mode ~ delay  | motif_pro + dist_tc_num + MDISTTOT_fin,
#                 weights = POND_JOUR,
#                 data = mobi_rum)
# summary(MOD_rum)

# Cas avec transit (dist_tc_num < 4 : station à moins de 15 min)
mobi_rum_tc <- dfidx(deploc_rum |> filter(dist_tc_num < 4 & DENSITECOM_RES!=4), 
                     shape = "wide", choice = "mode", varying = 6:9, sep = "_") |>
  dplyr::mutate(delay2= delay^2,
         DENSITECOM_RES = factor(DENSITECOM_RES))

MOD_rum_tc <- mlogit(mode ~ delay +delay2 | motif_pro + dist_tc_num + dist_tc_num2 + MDISTTOT_fin + DENSITECOM_RES ,
                     weights = POND_JOUR,
                     data = mobi_rum_tc)
summary(MOD_rum_tc)

# Cas sans transit (dist_tc_num = 4 : station à plus de 15 min)
mobi_rum_notc <- dfidx(deploc_rum |> 
                         filter(dist_tc_num == 4|DENSITECOM_RES==4, mode != "transit") |> 
                         select(-dist_tc_num, -delay_transit), 
                       shape = "wide", choice = "mode", varying = 5:7, sep = "_") |> 
  dplyr::mutate(delay2 = delay^2)

MOD_rum_notc <- mlogit(mode ~ delay +delay2 | motif_pro  + MDISTTOT_fin ,
                       weights = POND_JOUR,
                       data = mobi_rum_notc)
summary(MOD_rum_notc)

# Calcul simple de la congestion selon les densités urbaines des communes de départ et d'arrivée
quick_congestion <- deploc |> 
  dplyr::mutate(delay_car = DUREE - tt_car) |> 
  group_by(DENSITECOM_ORI, DENSITECOM_DES) |> 
  summarise(delay_car = median(delay_car)) |> 
  dplyr::mutate(densdes = factor(DENSITECOM_DES), 
         densori = factor(DENSITECOM_ORI))

MOD_congestion <- lm(delay_car ~ DENSITECOM_ORI + DENSITECOM_DES, data = quick_congestion)
MOD_congestion_f <- lm(delay_car ~ densori + densdes, data = quick_congestion)
summary(MOD_congestion)
summary(MOD_congestion_f)
# densite_urb <- readxl::read_xlsx("~/files/DVFdata/sources/Communes/grid.xlsx",
#                                  skip = 4)
# 
# densite_urb <- densite_urb |> 
#   select(-libgeo) |> 
#   dplyr::mutate(across(.fns = as.integer)) |> 
#   as.dplyr()

# Injection du modèle dans la matrice des distances 
distances <- arrow::read_parquet("/scratch/distances/distances_emplois.parquet")

# distances <- densite_urb[distances, on = c("codgeo"="COMMUNE") ] |> 
#   setnames(c("codgeo", "gridens"), c("COMMUNE", "DENSITECOM_ORI"))
# 
# distances <- densite_urb[distances, on = c("codgeo"="DCLT") ] |> 
#   setnames(c("codgeo", "gridens"), c("DCLT", "DENSITECOM_DES"))
RhpcBLASctl::blas_set_num_threads(4)
source("Mobilités des Personnes 2019/f.newpredict.r")
tf_car <- 7
distances[, `:=`(densdes = factor(DENSITECOM_DES),
                 densori = factor(DENSITECOM_ORI))]
distances[, `:=`(tt_car = travel_time_car,
                 travel_time_car = travel_time_car+tf_car)]
distances[, `:=`(delay_walk = travel_time_walk - travel_time_car,
                 delay_bike = travel_time_bike - travel_time_car,
                 delay_transit = travel_time_transit - travel_time_car,
                 delay_car = predict(MOD_congestion, newdata = distances),
                 delay_car_f = predict(MOD_congestion_f, newdata = distances))]

distances[, id := 1:.N]
distances[, dist_tc_num := cut(time2stop, breaks = c(0, 5, 10, 20, Inf), include.lowest = TRUE,
                               labels = 1:4) |> as.numeric()]
distances[, dist_tc_num2 := dist_tc_num^2]

distances[ , delay_car := delay_car_f]

# On n'utilise pas l'info sur les distances différentes par mode.
# pris indirectement en compte par les delay
newdata_tc <- distances[ dist_tc_num < 4 & DENSITECOM_ORI != 4, .(delay_walk, delay_bike, delay_transit, delay_car, 
                                                                  dist_tc_num, dist_tc_num2, MDISTTOT_fin = distance_car/1000, 
                                                                  DENSITECOM_RES=factor(DENSITECOM_ORI), id)]
ids_tc <- newdata_tc$id
newdata_tc[, `:=`(motif_pro = "motifs professionnels",
                  mode = "car",
                  POND_JOUR = 1)]
# Pour d'obscures raisons il faut rajouter une ligne des old data pour faire le predict
newdata_tc <- bind_rows(deploc_rum |> select(-dist_tc, -DENSITECOM_DES, -DENSITECOM_ORI, -dist_tc_so) |> dplyr::mutate(POND_JOUR = 0) |> slice(1), 
                        newdata_tc) 

newdata_tc <- dfidx(as_tibble(newdata_tc), shape = "wide", choice = "mode", varying = 5:8, sep = "_") |>
  dplyr::mutate(delay2 = delay^2,
         DENSITECOM_RES=factor(DENSITECOM_RES))

prediction_tc <- newpredict(MOD_rum_tc, newdata = newdata_tc)[-1,]
prediction_tc <- as.data.table(prediction_tc) |> 
  cbind(id = ids_tc)

# sans transit
newdata_notc <- distances[ dist_tc_num == 4 | DENSITECOM_ORI == 4, .(delay_walk, delay_bike, delay_car, 
                                                                     dist_tc_num, MDISTTOT_fin = distance_car/1000, id)]
ids_notc <- newdata_notc$id
newdata_notc[, `:=`(motif_pro = "motifs professionnels",
                    mode = "car",
                    POND_JOUR = 1)]
# Pour d'obscures raisons il faut rajouter une ligne des old data pour faire le predict
newdata_notc <- deploc_rum |>
  filter(dist_tc_num == 4, mode != "transit") |> 
  select(-dist_tc, -dist_tc_num, -delay_transit, -DENSITECOM_DES, -DENSITECOM_ORI, -DENSITECOM_RES, -dist_tc_so) |> 
  dplyr::mutate(POND_JOUR = 0) |> 
  slice(1) |> 
  bind_rows(newdata_notc) 

newdata_notc <- dfidx(newdata_notc, shape = "wide", choice = "mode", varying = 4:6, sep = "_") |> dplyr::mutate(delay2=delay^2)

prediction_notc <- newpredict(MOD_rum_notc, newdata = newdata_notc)[-1,]
prediction_notc <- as.data.table(prediction_notc) |> 
  cbind(id=ids_notc)
prediction_notc[, transit := 0]

predictions <- rbind(prediction_tc, prediction_notc)
setnames(predictions, transport, paste0("proba_", transport))

# Injection des proba de choix modal
distances <- predictions[distances, on = "id"]
# Calcul des effectifs (à partir des trajets fréquents capturés dans tout_mode)
distances[, (paste0(transport, "_pred")) := prop_ind * prop_emp * tout_mode * .SD, .SDcol = paste0("proba_", transport)]

# distances[, lapply(.SD, sum, na.rm = TRUE), .SDcol = paste0(transport, "_pred")]
print(nrow(unique(distances, by="fromidINS"))*nrow(unique(distances, by="toidINS")))
arrow::write_parquet(distances, "/scratch/distances/distances_emplois.parquet")

#après ce fichier là, on doit utiliser le code de "input_nb_trajets"