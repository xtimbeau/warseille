setwd("~/marseille")
library(readxl)
library(sp)
library(data.table)
library(sf)
library(qs)
library(archive) 
install.packages("remotes")
remotes::install_github("OFCE/r3035")
library(r3035)
library(tidyverse)
library(dplyr)
library(conflicted)
library(stars)
library(ggplot2)
library(units)
library(tmap)
conflict_prefer("filter", "dplyr")
conflict_prefer("select", "dplyr")
conflict_prefer("idINS2square", "accessibility")

load("baselayer.rda")
download = TRUE
geographie = TRUE

# On télécharge la BDD contenant l'ensemble des établissements scolaires via le site du ministère de l'EN
# On dispose alors des établissements, géolocalisés, de 1er et de 2è cycle, ainsi que ceux de l'enseignement supérieur

# On peut aussi télécharger les effectifs des établissements, qui pourraient être utiles pour estimer/calibrer le modèle

#download.file(url = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&csv_separator=%3B", destfile = "/scratch/DVFdata/sources/Scol/etablissements.csv")
#download.file(url = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-lycee_gt-effectifs-niveau-sexe-lv/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&csv_separator=%3B", destfile = "/scratch/DVFdata/sources/Scol/effectifs_LGT.csv")
#download.file(url = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-lycee_pro-effectifs-niveau-sexe-lv/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&csv_separator=%3B", destfile = "/scratch/DVFdata/sources/Scol/effectifs_LP.csv")
#download.file(url = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-college-effectifs-niveau-sexe-lv/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&csv_separator=%3B", destfile = "/scratch/DVFdata/sources/Scol/effectifs_collège.csv")
#download.file(url = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-ecoles-effectifs-nb_classes/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&csv_separator=%3B", destfile = "/scratch/DVFdata/sources/Scol/effectifs_écoles.csv")

#download.file(url = "https://www.data.gouv.fr/fr/datasets/r/63a7a69c-0155-4a8e-a73b-2009ee92eda2", destfile = "/scratch/DVFdata/sources/BPE/bpe2018.csv")
#download.file(url = "https://www.insee.fr/fr/statistiques/fichier/5395746/RP2018_mobsco_csv.zip", destfile = "~/files/larochelle/Mobsco")
#unzip("~/files/larochelle/Mobsco", exdir = "~/files/larochelle/")
#file.copy(from = paste0("~/files/larochelle/", "FD_MOBSCO_2018.csv"), to = paste0("/scratch/DVFdata/sources/MOBSCO/", "FD_MOBSCO_2018.csv"), overwrite = TRUE)
#file.copy(from = paste0("~/files/larochelle/", "Varmod_MOBSCO_2018.csv"), to = paste0("/scratch/DVFdata/sources/MOBSCO/", "Varmod_MOBSCO_2018.csv"), overwrite = TRUE)

#download.file(url = "https://www.insee.fr/fr/statistiques/fichier/5650720/base-ic-evol-struct-pop-2018_csv.zip", destfile =  "~/files/larochelle/irispop")
#unzip("~/files/larochelle/irispop", exdir = "~/files/larochelle/")
#file.copy(from = paste0("~/files/larochelle/", "base-ic-evol-struct-pop-2018.CSV"), to = paste0("/scratch/DVFdata/sources/iris/", "base-ic-evol-struct-pop-2018.csv"), overwrite = TRUE)
#file.copy(from = paste0("~/files/larochelle/", "meta_base-ic-evol-struct-pop-2018.CSV"), to = paste0("/scratch/DVFdata/sources/iris/", "meta_base-ic-evol-struct-pop-2018.csv"), overwrite = TRUE)

#download.file(url = "https://www.collectivites-locales.gouv.fr/files/Accueil/DESL/2021/Interco/epcicom2021.xlsx", destfile =  "{localdata}/autres/Intercommunalite_Metropole_au_01-01-2021.xls", overwrote = TRUE)

## Définition des zones
if (geographie){
  interco <- read_excel("~/files/Intercommunalite_Metropole_au_01-01-2017.xls", skip = 5)
  communes <- read_excel("~/files/table-appartenance-geo-communes-23.xlsx", skip = 5)
  communes_shape <- st_read("~/files/communes-20210101.shp") |> 
    st_set_crs(2154) |> 
    st_transform(3035)
  
  com21 <- st_read("~/files/communes-20210101.shp") |>
    sf::st_transform(3035)
  
  com17 <- st_read("~/files/communes-20170112.shp") |>
    sf::st_transform(3035)
  
  #com17$code21 <- com21$insee[st_nearest_feature(com17, com21)]
  
  # epcis <- readxl::read_xls(
  #   "~/files/Intercommunalite_Metropole_au_01-01-2017.xls" |> glue(),
  #   sheet=2,
  #   skip=5)
  # 
  # 
  # scot_tot.epci <- epcis |> 
  #   dplyr::filter(str_detect(LIBEPCI, str_c(scot_tot.n, collapse='|'))) |>
  #   pull(CODGEO, name=LIBGEO)
  # 
  # # 
  # scot3.epci21 <- epcis21 |> 
  #   dplyr::filter(str_detect(raison_sociale, "Aunis|Rochelle")) |>
  #   pull(insee, name=nom_membre)
  
  # epci3 <- interco |> 
  #   dplyr::filter(str_detect(LIBEPCI, "Aunis|Rochelle")) |> 
  #   pull(EPCI) 
  # iris <- qs::qread("{DVFdata}/iris18.qs" |> glue())
  # iris <- iris18  |> 
  #   rename(COM = DEPCOM, IRIS = CODE_IRIS) 
  # scot_tot <- iris |>
  #   dplyr::filter(LIB_IRIS %in% scot_tot.n) |>
  #   st_union() 
  # 
  
  
  zone_emploi <- iris |> filter(LIBCOM %in% scot_tot.n & DEP %in% c("13","30","83","84")) |> st_union() |> st_buffer(33000)
  
  com_ze <- com17 |> filter(st_intersects(com17, zone_emploi, sparse = FALSE))
  com_ze21 <- com21 |> filter(st_intersects(com21, zone_emploi, sparse = FALSE))
  
  communes.scot_tot <- iris |> filter(LIBCOM %in% scot_tot.n & DEP %in% c("13","30","83","84")) |>
    group_by(LIBCOM) |>
    summarize()
  
  
  # Le tmap était un test pour vérifier la bonne construction des zones, a priori c'est ok
  
  tmap <- tm_shape(com_ze) + tm_fill(col = 'red', alpha = 0, title = "nom") + tm_shape(com_ze) + tm_borders(col = 'red') + tm_shape(zone_emploi) + tm_borders() +
    tm_shape(communes.scot_tot) + tm_borders(col = 'green')
  tmap21 <- tm_shape(com_ze21) + tm_fill(col = 'red', alpha = 0, title = "nom") + tm_shape(com_ze21) + tm_borders(col = 'red') + tm_shape(zone_emploi) + tm_borders() +
    tm_shape(communes.scot_tot) + tm_borders(col = 'green')
tmap 
  }
# 
# centre <- communes_shape |> filter(nom == "La Rochelle", insee == "17300") |> st_centroid()
# in50km <- st_buffer(centre, dist = set_units(33, km))
#communes_in33km <- st_intersects(zone_emploi, communes_shape) # |> pull(insee) |> as.integer()

#iris <- read_excel("/scratch/DVFdata/sources/iris/2018/disp2018.xlsx")

#effectif_lycee_gt <- fread("/scratch/DVFdata/sources/Scol/effectifs_LGT.csv") |> select(Département, Commune, "Numéro du lycée","Nombre d'élèves")
#effectif_lycee_pro <- fread("/scratch/DVFdata/sources/Scol/effectifs_LP.csv") |> select(Département, Commune, "Numéro du lycée","Nombre d'élèves")
#effectif_college <- fread("/scratch/DVFdata/sources/Scol/effectifs_collège.csv") |> select(Département, Commune, "Numéro du collège","Nombre d'élèves")
#effectif_ecole <- fread("/scratch/DVFdata/sources/Scol/effectifs_écoles.csv")

#bpe <- fread("/scratch/DVFdata/sources/BPE/bpe2018.csv")

## ---  CONSTRUCTION C200 --------------------

# On reprend c200_file; celui-ci contient déjà tous les individus

# c200 <- qread(file = c200_file)
c200$etud <- c200$collegienslyceens + c200$ecoliers_mat + c200$ecoliers_prim
c200 <- c200 |> dplyr::filter(com %in% com_ze$insee)

# On importe les établissements géolocalisés de la base de données RAMSES
# Les établissements sont associés à la commune version 2021

etablissements <- fread("/scratch/DVFdata/sources/Scol/etablissements.csv") |> select("Code établissement", "Code commune", "Commune", "Code nature", "Nature", "Longitude", "Latitude", "Code département", "Coordonnee X", "Coordonnee Y", "Qualité d'appariement", "EPSG") |> drop_na(Longitude) |> drop_na(Latitude) 
colnames(etablissements) = c("Etab", "COM", "Commune", "CODNAT", "Nature", "longitude", "latitude", "Département", "X", "Y", "QualApp", "EPSG")

etablissements <- subset(etablissements, as.integer(Département) %in% c(016,085,017,079))
etablissements <- st_as_sf(etablissements,coords = c("X","Y"), crs = 2154, remove = FALSE)

etablissements <- etablissements |> st_transform(3035) 
etablissements$idINS <- idINS3035(etablissements |> st_coordinates(), resolution=200)
etablissements$idINS <- c200$idINS[st_nearest_feature(etablissements, c200)] 


scol_opport <- etablissements |> st_drop_geometry() |>  dplyr::filter((COM %in% com_ze21$insee) & (QualApp != "Mauvaise")) |> 
  select(idINS, COM) |> group_by(idINS) |> dplyr::count() |> 
  mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name="geometry") |> st_centroid()
## On associe les idINS au carreau dans c200 le plus proche; cela permet d'inclure les établissements situés dans des carreaux inhabités pour passer outre un problème de jointure spatiale

## On ignore pour le moment les établissements spéciaux type SEP, SEGPA, section agricole...
## Par ailleurs les sections de ce genre sont souvent situées au sein d'établissements généraux et seraient donc des points redondants?

# Il est possible qu'il y ait des erreurs dans la placement des points (voir variable "qualité d'appariement")

# On filtre spatialement pour ne garder que les établissements contenus dans zone_emploi et on construit les variables qui dénombrent les établissements dans chaque carreau (le group_by(idINS))
# On a un potentiel problème avec l'exhaustivité des données concernant les écoles maternelles (30% des flux de MOBSCO pour enfants entre 3 et 5 ans vont vers des 
# communes où aucune école maternelle n'est recensée). Deux pistes pour cela
#     1. Des écoles élémentaires accueillant les enfants de 3 à 10 ans mais pas déclarées comme deux établissements (maternelle et primaire) séparés
#     2. La scolarité en maternelle n'est pas obligatoire en France, donc les écoles privées hors contrat sont plus communes et moins bien recensées par le MEN ?

etablissements$lycees <- as.integer(etablissements$CODNAT %in% c(300, 301, 302, 306, 307, 320))#334, 380, 370
etablissements$colleges <- as.integer(etablissements$CODNAT %in% c(340))
etablissements$ecoles <- as.integer(etablissements$CODNAT %in% c(101, 102, 103, 151, 152, 153))
# etablissements$ecoles_prim <- as.integer(etablissements$CODNAT %in% c(151, 152, 153))
# etablissements$ecoles_mat <- as.integer(etablissements$CODNAT %in% c(101, 102, 103))

etab33km <- etablissements |> dplyr::filter(COM %in% com_ze21$insee & (QualApp != "Mauvaise"))

#et_c200 <- as.data.frame(etab33km) |> group_by(idINS) |> summarize(lycees = sum(lycees), colleges = sum(colleges), ecoles_prim = sum(ecoles_prim), ecoles_mat = sum(ecoles_mat))
et_c200 <- as.data.frame(etab33km) |> group_by(idINS) |> summarize(lycees = sum(lycees), colleges = sum(colleges), ecoles = sum(ecoles))

c200_edu <- left_join(c200, et_c200, by = 'idINS') 
c200_edu$totetab <- c200_edu$lycees + c200_edu$colleges + c200_edu$ecoles#_mat + c200_edu$ecoles_prim 

#On utilisera la géométrie 2021 pour les opérations futures sur nos données; la géométrie 2017 ne sert que pour la délimitation des communes du SCOT (join assez long)
c200_edu <- st_join(c200_edu, com21 |> select(com21 = insee), join=st_intersects, largest=TRUE)
c200_edu$scot <- c200_edu$com %in% communes.scot3$COM

#On modifie les noms : désormais 'com' se référera à la géographie 2021 (en fait on pourrait même supprimer la colonne 'com' originale)

names(c200_edu)[names(c200_edu) == 'com'] <- 'com17'
names(c200_edu)[names(c200_edu) == 'com21'] <- 'com'

#On construit des variable agrégées par communes pour la phase d'estimation avec le fichier détail MOBSCO
#IMPORTANT le format xxxxxx_com est réutilisé par des fonctions construite pour fusionner c200 avec des données d'enquête, donc il doit être respecté

c200grp <- c200_edu |> group_by(com) |> summarise(collegienslyceens_com = sum(collegienslyceens), ecoliers_mat_com = sum(ecoliers_mat), ecoliers_prim_com = sum(ecoliers_prim), etud_com = sum(etud))
c200_edu <- left_join(c200_edu, c200grp |> st_drop_geometry(), by = "com")


#On se débarasse des variables dont on ne se servira pas a priori
c200_edu <- c200_edu |> select(-ind, -men, -enfants, -adultes, -ind_18_64)
c200_edu <- c200_edu |> mutate(ecoliers = ecoliers_mat+ecoliers_prim, ecoliers_com = ecoliers_mat_com+ecoliers_prim_com)

save(com_ze, com_ze21, com17, com21, zone_emploi, communes.scot_tot, etab33km, file = "geographie.rda")
qsave(c200_edu, c200edu_init_file)
