library(tidyverse)
library(glue)
library(sf)
library(data.table)
library(mpcmp)
library(quantreg)
library(arrow)
library(furrr)
library(logger)
conflicted::conflict_prefer_all("dplyr", quiet = TRUE)
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbExecute(con, 'SET temp_directory = "/tmp"')
DBI::dbExecute(con, 'SET max_temp_directory_size = "50GB"')
DBI::dbExecute(con, 'SET max_memory = "40GB"')
# DBI::dbExecute(con, 'SET threads to 4')

source("projection/f.newpredict.r")
source("projection/f.projection.r")

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
  DBI::dbExecute(con, sql)
}

#load("baselayer.rda")

source("mglobals.r")
# A rechercher eq de load(empetact_file) # pour fichier mobpro, pour virer les flux intercommunaux nuls.

dir.create(glue::glue("logs"), showWarnings = FALSE, recursive = TRUE)
timestamp <- lubridate::stamp("15-01-20 10h08.05", orders = "dmy HMS", quiet = TRUE) (lubridate::now(tzone = "Europe/Paris"))
logfile <- glue::glue("logs/projection.{timestamp}.log")
logger::log_appender(logger::appender_file(logfile))

mobpro <- qs::qread(mobpro_file)

## Les modèles de pratiques de mobilité :
# (1) la fréquence journalière des boucles(cf. script Modele_Frequence_Boucles.r)
MOD_frequence_boucle <- readRDS("projection/MOD_frequence_boucles.rda")

# (2) la probabilité d'une boucle simple (cf. script Modeles_Longueurs_Boucles.r)
MOD_boucle_simple <- readRDS("projection/MOD_boucle_simple.rda")

# (3) Le ratio kilométrique du détour lors d'une boucle complexe (cf. script Modeles_Longueurs_Boucles.r)
MOD_prop_detours <- readRDS("projection/MOD_prop_detours.rda")

# (4) les choix modaux (cf. script Modeles_Choix_Modaux.r)
MOD_choix_modaux_tc <- readRDS("projection/MOD_choix_modaux_tc.rda")
MOD_choix_modaux_notc <- readRDS("projection/MOD_choix_modaux_notc.rda")

## Les données sociodemo
c200ze <- qs::qread(c200ze_file)
iris4mod <- qs::qread("/space_mounts/data/marseille/iris4mod.qs") |> filter(ind>0)

# commune <- qs::qread(communes_file) |> 
#   mutate(across(-geometry, .fns = factor),
#          codgeo = insee) |> 
#   st_drop_geometry()

# recherche commune pour 
densites_communes <- "/space_mounts/data/DVFdata/sources/Communes/grid.xlsx"
densite <- readxl::read_xlsx(densites_communes)
names(densite) <- c('codgeo', "libelle", "gridens")
iris4mod <- iris4mod |> 
  st_drop_geometry() |> 
  mutate(codgeo = str_sub(IRIS,1,5)) |> 
  mutate(codgeo = ifelse(str_detect(codgeo, "^132"), "13055", codgeo)) |> 
  left_join(densite |> select(codgeo, densite=gridens), by="codgeo") |> 
  mutate(DENSITECOM_RES = case_when(
    densite == 1 ~ "Dense",
    densite == 2 ~ "Assez dense",
    densite == 3 ~ "Peu dense",
    densite == 4 ~ "Peu dense",
    TRUE ~ NA_character_) |> factor(levels = c("Dense", "Assez dense", "Peu dense"))) 

type_menage <- iris4mod |> 
  select(idINS, starts_with("ti")) |> 
  pivot_longer(cols = starts_with("ti"), names_to = "TYPMEN5", names_prefix = "ti", values_to = "poids") |> 
  mutate(TYPMEN5 = factor(TYPMEN5, labels = c("Personne seule", "Couple avec enfant(s)", "Couple sans enfant", 
                                              "Monoparent", "Autres")) |> 
           fct_relevel("Monoparent", "Couple sans enfant", after = 1L)) |>
  filter(poids > 0) # Attention valeur négative sur ti5

voiture <- iris4mod |> 
  transmute(idINS, v1 = voiture * ind_18_64, v0 = (1 - voiture) * ind_18_64) |> 
  pivot_longer(cols = -idINS, names_to = "voiture", names_prefix = "v", values_to = "poids") |> 
  mutate(voiture = as.integer(voiture)) |> 
  drop_na()
  
donnees_categoriques <- remplir_categories(type_menage, voiture) |> drop_na()
donnees_simples <- iris4mod |> select(idINS, IRIS, DENSITECOM_RES, revuce)

donnees <- left_join(donnees_categoriques, donnees_simples, by = "idINS") |> 
  mutate(fromidINS = idINS,
         voiture = as.logical(voiture)) |> 
  select(-idINS, -IRIS) |> 
  group_by(fromidINS) |> 
  mutate(poids = poids / sum(poids)) |> # On se ramène à un taux pour un actif
  ungroup() |> 
  setDT()

setkey(donnees, "fromidINS")

# Procédure iris par iris
les_morceaux <- iris4mod$IRIS |> unique()

# on retire les "85"
# les_iris <- discard(les_iris, \(x) str_detect(x, "^85"))

flux_mobpro <- mobpro[, .(NB = sum(NB_in)), by = c("COMMUNE", "DCLT")][NB>0,]

id2iris_from <- c200ze |> 
  filter(scot, ind>0) |> 
  st_drop_geometry() |> 
  select(fromidINS = idINS, IRIS) |> 
  mutate(IRIS = as.integer(IRIS))

id2iris_to <- c200ze |> 
  filter(emp>0) |> 
  st_drop_geometry() |> 
  select(toidINS = idINS, IRIS) |> 
  mutate(IRIS = as.integer(IRIS))

# distances <- open_dataset(dist_dts) |> 
#   filter(COMMUNE=="13001" | COMMUNE =="13211" | COMMUNE == "13005") |> 
#   select(fromidINS, toidINS, travel_time, distance, mode) |> 
#   collect()
# 
# distances[, mode := fcase(
#   mode == "walk_tblr", "walk",
#   mode == "bike_tblr", "bike",
#   mode == "transit", "transit",
#   mode == "car_dgr", "car"
# ) |> factor()]
# 
# distances[, distance := distance / 1000] # passage en Km
# 
# distances <- distances[sample(163100027, size=5000)]
# 
# dist_car <- distances[mode=="car", .(fromidINS, toidINS, distance)]
# tt_temp <- dcast(distances, fromidINS + toidINS ~ mode, value.var = "travel_time")
# tt_temp[, euc := r3035::idINS2dist(tt_temp$fromidINS, tt_temp$toidINS)/1000]
# 
# distances <- merge(tt_temp, dist_car, by = c("fromidINS", "toidINS"), all.x = TRUE)
# rm(dist_car, tt_temp)
# 
# coef_distance <- lm(distance - .1 ~ -1 + euc, data = distances)$coefficient 
# coef_walk <- lm(walk ~ -1 + euc, data = distances)$coefficient 
# coef_bike <- lm(bike ~ -1 + euc, data = distances)$coefficient
# coef_transit <- lm(transit ~ -1 + euc, data = distances)$coefficient
# coef_car <- lm(car ~ -1 + euc, data = distances)$coefficient
# 
# rm(distances)

coef_distance <- 1.473 
coef_walk <- 17.55
coef_bike <- 5.721
coef_transit <- 3.672
coef_car <- 1.334

# Création des dossiers
dir_mar <- "/space_mounts/data/marseille"
if(FALSE) {
unlink("{dir_mar}/delta_iris" |> glue(), recursive = TRUE)
dir.create("{dir_mar}/delta_iris" |> glue())
walk(les_morceaux, \(x) dir.create("{dir_mar}/delta_iris/IRIS={x}" |> glue()))
}

already_done <- map_lgl(les_morceaux, \(x) {
  z <- list.files("{dir_mar}/delta_iris/IRIS={x}" |> glue())
  return(length(z) >= 1)
})

plan("multisession", workers = 1L)
#options(future.globals.maxSize=3*1024^3)

id2iris_from <- id2iris_from |> to_duckdb()
id2iris_to <- id2iris_to |> to_duckdb()

mb95 <- mobpro |> 
  filter(mobpro95) |> 
  select(COMMUNE, DCLT, mobpro95) |> 
  to_duckdb()

if(FALSE) {
les_distances <- open_dataset(dist_dts) |>
  to_duckdb() |>
  select(fromidINS, toidINS, travel_time, distance, mode, access_time, n_rides, COMMUNE, DCLT) |>
  semi_join(mb95, by = c("COMMUNE", "DCLT")) |>
  semi_join(id2iris_to, by = "toidINS" ) |> 
  select(fromidINS, toidINS, travel_time, distance, mode, access_time, n_rides) |>
  left_join(id2iris_from, by = "fromidINS") |> 
  compute() |>
  duckdb2parquet("distances_packed", by = "IRIS")
rm(les_distances)
}

les_distances <- open_dataset("distances_packed") |> 
  to_duckdb()

walk(les_morceaux[!already_done], \(un_iris) {
  library(SparseM)
  library(mpcmp)
  library(quantreg)
  # les_identifiants <- open_dataset(idINS_emp_file) |> 
  #   filter(fromIris == un_iris, emp == TRUE) |> 
  #   select(id, toidINS, fromidINS, fromIris, COMMUNE, DCLT) |> 
  #   collect()
  # 
  # les_identifiants <- semi_join(les_identifiants, mobpro, by = c("COMMUNE", "DCLT")) |> 
  #   select(-c(COMMUNE, DCLT))
  # 
  # distances <- inner_join(les_identifiants, les_distances, by = "id") |> 
  #   setDT()
  # 
  la_commune <- str_sub(un_iris, 1,5) |> as.integer()
  
  distances <- les_distances |> 
    filter(IRIS == un_iris) |>
    collect() |> 
    setDT()
  
  log_info("IRIS {un_iris}, {nrow(distances)} paires de distance originaires")
  
  if (nrow(distances) == 0) {
    cli::cli_alert_warning("Pas de distances iris {un_iris}")
    return(NULL)
  }
  
  les_stations <- distances[mode=="transit", .(fromidINS, access_time, n_rides)]
  les_stations[, dist_tc_proche := fifelse(access_time/60 <= 12 & n_rides > 0, TRUE, FALSE, na = FALSE)]
  les_stations <- les_stations[, .(dist_tc_proche = any(dist_tc_proche)), by = .(fromidINS)]
  
  distances <- distances[les_stations, on = "fromidINS"]
  
  distances[, `:=`(access_time = NULL,
                   n_rides = NULL,
                   mode = fcase(
                     mode == "walk_tblr", "walk",
                     mode == "bike_tblr", "bike",
                     mode == "transit", "transit",
                     mode == "car_dgr", "car"
                   ) |> factor(),
                   dist_tc_proche = replace_na(dist_tc_proche, FALSE))]
  
  # setnames(distances, 
  #          c("distance_car", "travel_time_car", "travel_time_walk", "travel_time_bike", "travel_time_transit"),
  #          c("distance", "tt_car", "tt_walk", "tt_bike", "tt_transit")) 
  
  distances[, `:=`(distance = distance / 1000,
                   travel_time = travel_time/ 60) ] # passage en Km et en minutes
  
  dist_car <- distances[mode=="car", .(fromidINS, toidINS, distance)]
  tt_temp <- dcast(distances, fromidINS + toidINS + dist_tc_proche ~ mode, value.var = "travel_time")
  tt_temp[, euc := r3035::sidINS2dist(tt_temp$fromidINS, tt_temp$toidINS)/1000]
  
  distances <- merge(tt_temp, dist_car, by = c("fromidINS", "toidINS"), all.x = TRUE)
  rm(dist_car, tt_temp)
  gc()
  distances[, ':='(distance = fifelse(is.na(distance), .1 + coef_distance * euc, distance),
                   tt_walk = fifelse(is.na(walk), round(coef_walk * euc), walk),
                   tt_bike = fifelse(is.na(bike), round(coef_bike * euc), bike),
                   tt_transit = fifelse(is.na(transit), round(coef_transit * euc), transit),
                   tt_car = fifelse(is.na(car), round(coef_car * euc), car))]
  
  # distances |> slice_sample(n=1000) |> ggplot(aes(x=distance)) +
  #   geom_point(aes(y = tt_walk), col = "blue") +
  #   geom_point(aes(y = tt_bike), col = "yellow") +
  #   geom_point(aes(y = tt_transit), col = "green") +
  #   geom_point(aes(y = tt_car), col = "red")
  
  distances[, ':='(bike = NULL, walk = NULL, transit = NULL, car = NULL, IRIS = NULL)]
  
  setkey(distances, "fromidINS")
  
  #distances <- split(distances, distances$COMMUNE)
  
  # i = origine ; j = destination ; k = catégories usagers ; m = mode de transport ; bcl = boucle
  # KM(i,j,k,m) = LONGUEUR_BOUCLE(bcl,k,m) * FREQUENCE(bcl,k,m)  * RUM(bcl, k, m)
  # Pour bcl, on sépare le calcul entre les cas simples et les cas complexe
  # pour Prob_bcl_simple(k) on prend dist_bcl = 2 * DISTANCE(i,j) 
  # pour (1 - Prob_bcl_simple(k)) on prend dist_bcl =  K(i,j) * DISTANCE(i,j)
  
  dist <- merge(distances, donnees, by = "fromidINS", all.y = FALSE, all.x = FALSE, allow.cartesian = TRUE)
  
  logger::log_info("...Merged avec donnees : {nrow(dist)} lignes")
  
  logger::log_info("{dist |> group_by(TYPMEN5) |> summarize(sum(poids)) |> knitr::kable()}")
  logger::log_info("{dist |> group_by(voiture) |> summarize(sum(poids)) |> knitr::kable()}")
  
  # il semblerait qu'il puisse y avoir un pb ici avec parfois 0 lignes
  if (nrow(dist) == 0) {
    cli::cli_alert_warning("pas de données pour l'iris {un_iris}")
    return(NULL)
  }
  # cli::cli_alert_info("Iris {un_iris} : {nrow(dist)} lignes.")
  # BOUCLE simple 
  prob_simple <- predict(MOD_boucle_simple, newdata = dist, type = "response") |> unname()
  
  # BOUCLE complexe
  # ATTENTION : la formule exacte dépend des changements de variables lors de l'estimation.
  # A ADAPTER en fonction du modèle.
  K <- 2 + exp(predict(MOD_prop_detours, newdata = dist) |> unname()) - .01
  
  # scénario des distances
  dist[distance == 0, `:=`(distance=1, tt_walk=1, tt_bike=5, tt_transit=5, tt_car = 10)]
  distance_domicile_travail <- dist$distance
  dist[, distance := NULL]
  # les longueurs des boucles
  longueur_simple <- 2 * distance_domicile_travail
  longueur_complex <- K * distance_domicile_travail
  
  # FREQ. La prédiction est journalière. On veut à l'année.
  pred_freq_simple <- 365 * predict(MOD_frequence_boucle, newdata = cbind(dist, distance = longueur_simple), type = "response") |> unname()
  pred_freq_complex <- 365 * predict(MOD_frequence_boucle, newdata = cbind(dist, distance = longueur_complex), type = "response") |> unname()
  
  # RUM
  indices_tc <- dist$dist_tc_proche
  
  dist <- split(dist, by = "dist_tc_proche")
  
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
    gc()
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
    gc()
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
  fn <- "/space_mounts/data/marseille/delta_iris/IRIS={un_iris}/part-0.parquet" |> glue()
  log_info("...Projection écrite {fn}")
  write_parquet(dist, fn)
  rm(dist)
  gc()
}, .progress=TRUE)

cli::cli_alert_info("le dataset {dir_mar}/delta a été écrit")




