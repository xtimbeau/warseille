library(tidyverse)
library(glue)
library(sf)
library(data.table)
library(mpcmp)
library(quantreg)
library(arrow)
library(furrr)
conflicted::conflict_prefer_all("dplyr", quiet = TRUE)

source("Mobilités des Personnes 2019/f.newpredict.r")
source("mod/f.projection.r")

load("baselayer.rda")
load(empetact_file) # pour fichier mobpro, pour virer les flux intercommunaux nuls.

## Les modèles de pratiques de mobilité :
# (1) la fréquence journalière des boucles(cf. script Modele_Frequence_Boucles.r)
MOD_frequence_boucle <- readRDS("mod/MOD_frequence_boucles.rda")

# (2) la probabilité d'une boucle simple (cf. script Modeles_Longueurs_Boucles.r)
MOD_boucle_simple <- readRDS("mod/MOD_boucle_simple.rda")

# (3) Le ratio kilométrique du détour lors d'une boucle complexe (cf. script Modeles_Longueurs_Boucles.r)
MOD_prop_detours <- readRDS("mod/MOD_prop_detours.rda")

# (4) les choix modaux (cf. script Modeles_Choix_Modaux.r)
MOD_choix_modaux_tc <- readRDS("mod/MOD_choix_modaux_tc.rda")
MOD_choix_modaux_notc <- readRDS("mod/MOD_choix_modaux_notc.rda")

## Les données sociodemo
c200ze <- qs::qread(c200ze_file)
iris4mod <- qs::qread("/scratch/distances/xt/iris4mod.qs")

commune <- qs::qread(commune_file) |> 
  mutate(across(-geometry, .fns = factor),
         codgeo = insee) |> 
  st_drop_geometry()

iris4mod <- iris4mod |> 
  st_drop_geometry() |> 
  mutate(codgeo = str_sub(CODE_IRIS,1,5)) |> 
  left_join(commune |> select(codgeo, densite), by="codgeo") |> 
  mutate(DENSITECOM_RES = case_when(
    densite == "Communes densément peuplées" ~ "Dense",
    densite == "Communes de densité intermédiaire" ~ "Assez dense",
    densite == "Communes peu denses" ~ "Peu dense",
    densite == "Communes très peu denses" ~ "Peu dense",
    TRUE ~ NA_character_) |> factor(levels = c("Dense", "Assez dense", "Peu dense"))) 

mean_ndv <- iris4mod |> summarise(revuce = weighted.mean(revuce, ind_18_64, na.rm = TRUE)) |> pull(revuce)

# On prend comme référence le cas majoritaire :
# TYPMEN5 Couple avec enfant(s)
# voiture TRUE
# revuce = mean_ndv
donnees <- iris4mod |> 
  transmute(fromidINS = factor(idINS),
         TYPMEN5 = "Couple avec enfant(s)",
         voiture = TRUE, 
         DENSITECOM_RES,
         poids = 1,
         revuce = mean_ndv) |> 
  mutate(TYPMEN5 = factor(TYPMEN5, 
                          levels = c("Personne seule", "Monoparent", "Couple sans enfant", 
                                     "Couple avec enfant(s)", "Autres"))) |> 
  setDT()

setkey(donnees, "fromidINS")

# Procédure iris par iris
les_iris <- iris4mod$CODE_IRIS |> unique()

# on retire les "85"
les_iris <- discard(les_iris, \(x) str_detect(x, "^85"))

les_distances <- read_parquet(distances_file,
                              col_select = c(id, distance_car, travel_time_car, travel_time_bike, 
                                             travel_time_walk, travel_time_transit, time2stop))

# Création des dossiers
dir <-"/scratch"
unlink("{dir}/deltaref" |> glue(), recursive = TRUE)
dir.create("{dir}/deltaref" |> glue())
walk(les_iris, \(x) dir.create("{dir}/deltaref/CODE_IRIS={x}" |> glue()))

already_done <- map_lgl(les_iris, \(x) {
  z <- list.files("{dir}/deltaref/CODE_IRIS={x}" |> glue())
  return(length(z) >= 1)
})

plan("multisession", workers = 4L)
options(future.globals.maxSize=3*1024^3)
future_walk(les_iris[!already_done], \(un_iris) {
  library(mpcmp)
  library(quantreg)
  dir <-"/scratch"
  les_identifiants <- open_dataset(idINS_emp_file) |> 
    filter(fromIris == un_iris, emp == TRUE) |> 
    select(id, toidINS, fromidINS, fromIris, COMMUNE, DCLT) |> 
    collect()
  
  les_identifiants <- semi_join(les_identifiants, mobpro, by = c("COMMUNE", "DCLT")) |> 
    select(-c(COMMUNE, DCLT))
  
  distances <- inner_join(les_identifiants, les_distances, by = "id") |> 
    setDT()
  
  # PATCH time2stop NA
  distances[, time2stop := replace_na(time2stop, min(time2stop, na.rm = TRUE)), by = .(fromidINS)]
  
  distances[, `:=`(fromidINS = factor(fromidINS),
                   toidINS = factor(toidINS),
                   dist_tc_proche = (time2stop <= 12))]
  
  setnames(distances, 
           c("distance_car", "travel_time_car", "travel_time_walk", "travel_time_bike", "travel_time_transit"),
           c("distance", "tt_car", "tt_walk", "tt_bike", "tt_transit")) 
  
  distances[, distance := distance / 1000] # passage en Km
  
  setkey(distances, "fromidINS")
  
  #distances <- split(distances, distances$COMMUNE)
  
  # i = origine ; j = destination ; k = catégories usagers ; m = mode de transport ; bcl = boucle
  # KM(i,j,k,m) = LONGUEUR_BOUCLE(bcl,k,m) * FREQUENCE(bcl,k,m)  * RUM(bcl, k, m)
  # Pour bcl, on sépare le calcul entre les cas simples et les cas complexe
  # pour Prob_bcl_simple(k) on prend dist_bcl = 2 * DISTANCE(i,j) 
  # pour (1 - Prob_bcl_simple(k)) on prend dist_bcl =  K(i,j) * DISTANCE(i,j)
  
  dist = merge(distances, donnees, by = "fromidINS", all.y = FALSE, all.x = FALSE, allow.cartesian = TRUE)
  
  # cli::cli_alert_info("Iris {un_iris} : {nrow(dist)} lignes.")
  # BOUCLE simple 
  prob_simple <- predict(MOD_boucle_simple, newdata = dist, type = "response")
  
  # BOUCLE complexe
  # ATTENTION : la formule exacte dépend des changements de variables lors de l'estimation.
  # A ADAPTER en fonction du modèle.
  K <- 2 + exp(predict(MOD_prop_detours, newdata = dist)) - .01
  
  # scénario des distances
  distance_domicile_travail <- dist$distance
  dist[, distance := NULL]
  
  # les longueurs des boucles
  longueur_simple <- 2 * distance_domicile_travail
  longueur_complex <- K * distance_domicile_travail
  
  # FREQ. La prédiction est journalière. On veut à l'année.
  pred_freq_simple <- 365 * predict(MOD_frequence_boucle, newdata = cbind(dist, distance = longueur_simple), type = "response")
  pred_freq_complex <- 365 * predict(MOD_frequence_boucle, newdata = cbind(dist, distance = longueur_complex), type = "response")
  
  # RUM
  dist[, tc_proche := (time2stop <= 12)]
  indices_tc <- dist$tc_proche
  
  dist <- split(dist, by = "tc_proche")
  
  # cli::cli_alert_info("Calcul des choix modaux.")
  # TC simple
  
  if (!is.null(dist$'TRUE')) {
    
    p_mode_tc_simple <- dist$`TRUE`[, .(tt_car, tt_walk, tt_bike, tt_transit, DENSITECOM_RES, TYPMEN5,voiture, revuce)] |> 
      cbind(ldistance = log(longueur_simple[indices_tc])) |>
      mutate(lrevuce = log(revuce)) |>
      select(-revuce) |> 
      newpredict_widedata(MOD_choix_modaux_tc, newdata=_)
    
    p_mode_tc_complex <- dist$`TRUE`[, .(tt_car, tt_walk, tt_bike, tt_transit, DENSITECOM_RES, TYPMEN5,voiture, revuce)] |> 
      cbind(ldistance = log(longueur_complex[indices_tc])) |> 
      mutate(lrevuce = log(revuce)) |> 
      select(-revuce) |> 
      newpredict_widedata(MOD_choix_modaux_tc, newdata=_)
    
    delta_tc <- (
      sweep(p_mode_tc_simple, MARGIN = 1, longueur_simple[indices_tc] * pred_freq_simple[indices_tc] * prob_simple[indices_tc], FUN = "*") +
        sweep(p_mode_tc_complex, MARGIN = 1, longueur_complex[indices_tc] * pred_freq_complex[indices_tc] * (1 - prob_simple[indices_tc]), FUN = "*") 
    ) |> 
      as.data.table()
    
    rm(p_mode_tc_simple, p_mode_tc_complex)
  }  
  
  # No TC simple
  if (!is.null(dist$'FALSE')) {
    
    p_mode_notc_simple <- dist$`FALSE`[, .(tt_car, tt_walk, tt_bike, DENSITECOM_RES, TYPMEN5,voiture, revuce)] |> 
      cbind(ldistance = log(longueur_simple[!indices_tc])) |> 
      mutate(lrevuce = log(revuce)) |> 
      newpredict_widedata(MOD_choix_modaux_notc, newdata=_)
    
    p_mode_notc_complex <- dist$`FALSE`[, .(tt_car, tt_walk, tt_bike, DENSITECOM_RES, TYPMEN5,voiture, revuce)] |> 
      cbind(ldistance = log(longueur_complex[!indices_tc])) |> 
      mutate(lrevuce = log(revuce)) |> 
      newpredict_widedata(MOD_choix_modaux_notc, newdata=_)
    
    delta_notc <- (
      sweep(p_mode_notc_simple, MARGIN = 1, longueur_simple[!indices_tc] * pred_freq_simple[!indices_tc] * prob_simple[!indices_tc], FUN = "*") +
        sweep(p_mode_notc_complex, MARGIN = 1, longueur_complex[!indices_tc] * pred_freq_complex[!indices_tc] * (1 - prob_simple[!indices_tc]), FUN = "*") 
    ) |> 
      as.data.table()
    
    rm(p_mode_notc_simple, p_mode_notc_complex)
  }
  
  # Synthèse
  # cli::cli_alert_info("Synthèse.")
  
  if (!is.null(dist$'TRUE')) {
    dist$`TRUE` <- cbind(dist$`TRUE`[, .(fromidINS, toidINS, poids)], delta_tc)
    dist$`TRUE` <- dist$`TRUE`[, lapply(.SD, \(x) sum(poids*x)), by = .(fromidINS, toidINS)]
  }
  
  if (!is.null(dist$'FALSE')) {
    dist$`FALSE` <- cbind(dist$`FALSE`[, .(fromidINS, toidINS, poids)], delta_notc)
    dist$`FALSE` <- dist$`FALSE`[, lapply(.SD, \(x) sum(x*poids)), by = .(fromidINS, toidINS)]
    dist$`FALSE`[, transit := 0]
  }
  
  dist <- rbindlist(dist, use.names = TRUE)
  
  write_parquet(dist, "{dir}/deltaref/CODE_IRIS={un_iris}/part-0.parquet" |> glue())
  rm(dist)
}, .progress=TRUE)

cli::cli_alert_info("le dataset {dir}/deltaref a été écrit")




