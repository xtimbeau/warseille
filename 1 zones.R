# init ---------------
setwd("~/marseille")
library(tidyverse)
library(glue)
library(conflicted)
library(rlang)
library(glue)
library(stringr)
library(pins)
library(AzureStor) 
library(writexl)


progressr::handlers(global = TRUE)
progressr::handlers(progressr::handler_progress(format = ":bar :percent :eta", width = 80))

bl <- NULL
if(fs::file_exists("baselayer.rda")) bl <- load("baselayer.rda")

# globals -----------------------------------------------------------------
ville <- "Marseille"
scot1.n <- c("Allauch",	"Carry-le-Rouet","Cassis", "Ceyreste","Châteauneuf-les-Martigues","La Ciotat","Ensuès-la-Redonne","Gémenos","Gignac-la-Nerthe","Marignane","Marseille","Plan-de-Cuques","Roquefort-la-Bédoule","Le Rove","Saint-Victoret","Sausset-les-Pins","Septèmes-les-Vallons","Carnoux-en-Provence")
scot2.n <- c("Aix-en-Provence","Beaurecueil","Bouc-Bel-Air","Cabriès","Châteauneuf-le-Rouge","Éguilles","Fuveau","Jouques","Lambesc","Meyrargues","Meyreuil","Mimet","Les Pennes-Mirabeau","Peynier","Peyrolles-en-Provence","Puyloubier","Le Puy-Sainte-Réparade","Rognes","La Roque-d'Anthéron","Rousset","Saint-Antonin-sur-Bayon","Saint-Cannat","Saint-Estève-Janson","Saint-Marc-Jaumegarde","Saint-Paul-lès-Durance","Simiane-Collongue","Le Tholonet","Trets","Vauvenargues","Venelles","Ventabren","Vitrolles","Coudoux","Pertuis")
scot3.n <- c("Alleins","Aurons","La Barben","Berre-l'Étang","Charleval","Eyguières","La Fare-les-Oliviers","Lamanon","Lançon-Provence","Mallemort","Pélissanne","Rognac","Saint-Chamas","Salon-de-Provence","Sénas","Velaux","Vernègues")
scot4.n <- c("Aubagne","Auriol","Belcodène","La Bouilladisse","Cadolive","Cuges-les-Pins","La Destrousse","Gréasque","La Penne-sur-Huveaune","Peypin","Roquevaire","Saint-Savournin","Saint-Zacharie")
scot5.n <- c("Arles","Aureille","Barbentane","Les Baux-de-Provence","Boulbon","Cabannes","Châteaurenard","Eygalières","Eyragues","Fontvieille","Graveson","Maillane","Mas-Blanc-des-Alpilles","Maussane-les-Alpilles","Saint-Pierre-de-Mézoargues","Mollégès","Mouriès","Noves","Paradou","Rognonas","Saint-Andiol","Saint-Étienne-du-Grès","Saintes-Maries-de-la-Mer","Saint-Martin-de-Crau","Saint-Rémy-de-Provence","Tarascon","Verquières")
scot5.n <- c("Cornillon-Confoux","Fos-sur-Mer","Grans","Istres","Martigues","Miramas","Port-de-Bouc","Port-Saint-Louis-du-Rhône","Saint-Mitre-les-Remparts")

scot_tot.n <- c("Allauch",	"Carry-le-Rouet","Cassis", "Ceyreste","Châteauneuf-les-Martigues","La Ciotat","Ensuès-la-Redonne","Gémenos","Gignac-la-Nerthe","Marignane","Marseille","Plan-de-Cuques","Roquefort-la-Bédoule","Le Rove","Saint-Victoret","Sausset-les-Pins","Septèmes-les-Vallons","Carnoux-en-Provence","Aix-en-Provence","Beaurecueil","Bouc-Bel-Air","Cabriès","Châteauneuf-le-Rouge","Éguilles","Fuveau","Jouques","Lambesc","Meyrargues","Meyreuil","Mimet","Les Pennes-Mirabeau","Peynier","Peyrolles-en-Provence","Puyloubier","Le Puy-Sainte-Réparade","Rognes","La Roque-d'Anthéron","Rousset","Saint-Antonin-sur-Bayon","Saint-Cannat","Saint-Estève-Janson","Saint-Marc-Jaumegarde","Saint-Paul-lès-Durance","Simiane-Collongue","Le Tholonet","Trets","Vauvenargues","Venelles","Ventabren","Vitrolles","Coudoux","Pertuis","Alleins","Aurons","La Barben","Berre-l'Étang","Charleval","Eyguières","La Fare-les-Oliviers","Lamanon","Lançon-Provence","Mallemort","Pélissanne","Rognac","Saint-Chamas","Salon-de-Provence","Sénas","Velaux","Vernègues","Aubagne","Auriol","Belcodène","La Bouilladisse","Cadolive","Cuges-les-Pins","La Destrousse","Gréasque","La Penne-sur-Huveaune","Peypin","Roquevaire","Saint-Savournin","Saint-Zacharie","Arles","Aureille","Barbentane","Les Baux-de-Provence","Boulbon","Cabannes","Châteaurenard","Eygalières","Eyragues","Fontvieille","Graveson","Maillane","Mas-Blanc-des-Alpilles","Maussane-les-Alpilles","Saint-Pierre-de-Mézoargues","Mollégès","Mouriès","Noves","Paradou","Rognonas","Saint-Andiol","Saint-Étienne-du-Grès","Saintes-Maries-de-la-Mer","Saint-Martin-de-Crau","Saint-Rémy-de-Provence","Tarascon","Verquières","Cornillon-Confoux","Fos-sur-Mer","Grans","Istres","Martigues","Miramas","Port-de-Bouc","Port-Saint-Louis-du-Rhône","Saint-Mitre-les-Remparts")

marseille_board <- pins::board_azure(
  AzureStor::storage_container(azure_url, sas = azure_jeton))

#à faire juste une fois
marseille_board %>% pin_upload("Mobilités des Personnes 2019")
marseille_board %>% pin_upload("emp33km.qs")
marseille_board %>% pin_upload("c200_17.qs")
marseille_board %>% pin_upload("grid.xlsx")
marseille_board %>% pin_upload("c200_stars.rda")
marseille_board %>% pin_upload("base-cc-emploi-pop-active-2018.xlsx")
marseille_board %>% pin_upload("~/files/marseille_parcs_jardins_2018.csv") 
marseille_board %>% pin_upload("gtfs_marseille_zip.zip") 
marseille_board %>% pin_upload("~/files/iris18.7z") 
marseille_board %>% pin_upload("~/files/reference_IRIS_geo2018.xls") 
marseille_board %>% pin_upload("~/files/carreaux.zip") 
marseille_board %>% pin_upload("~/files/iris18.xlsx") 
#marseille_board %>% pin_upload("~/files/c200.xslx") 
marseille_board %>% pin_upload("~/files/communes_2017.zip") 
marseille_board %>% pin_upload("~/files/communes_2021.zip")
marseille_board %>% pin_upload("~/files/Intercommunalite_Metropole_au_01-01-2017.xls")





# fichiers importants
localdata <- "~/files/marseille"
# DVFdata <- "/scratch/DVFdata"
scripts <- "~/marseille"
mob2019 <- marseille_board  %>% pin_download('Mobilités des Personnes 2019')
home <- "~/marseille"
#repository <- "/scratch"
temp_dir <- "~/temp"
r5files_rep <- "/scratch/distances/xt"
newpredict_file <- "~/marseille/annexes/newpredict.r"
enqmobstarter_file <- "~/marseille/mod/5.a enq_mob_starter.R"
mod_rep <- "/files/marseille"
output_rep <- "~/marseille/output/"
fs::dir_create(output_rep)


# distances, probas et tutti quanti

r5_output <- set_names(c("bike.parquet", "car5.parquet", "transit_ref.parquet", "walk.parquet"),
                       c("bike",         "car",          "transit",         "walk"))
repository_distances <- "{repository}/distances" |> glue()
repository_distances_scol <- "{repository_distances}/scol" |> glue()
repository_distances_emploi <- "{repository_distances}/emploi" |> glue()
idINS_emp_file <- "{repository_distances_emploi}/idINS.parquet" |> glue()
idINS_scol_file <- "{repository_distances_scol}/idINS.parquet" |> glue()
distances_file <- "{repository_distances}/xdistances.parquet" |> glue()
distances_scol_file <- "{repository_distances}/xdistances_scol.parquet" |> glue()
proproba_file <- "{repository_distances}/proba_pro.parquet" |> glue()
nonproproba_file <- "{repository_distances}/proba_nonpro.parquet" |> glue()
kmpro_file <- "{repository_distances}/km_pro.parquet" |> glue()
kmscol_file <- "{repository_distances}/km_scol.parquet" |> glue()
kmscol_ppo_file <- "{repository_distances}/km_scol_ppo.parquet" |> glue()
kmnonpro_file <- "{repository_distances}/km_nonpro.parquet" |> glue()
meaps_rep <- "{repository_distances}/meaps" |> glue()
dir.create(meaps_rep)
meaps_file <- "{meaps_rep}/meaps_est.rda" |> glue()
decor_carte_file <- "{repository_distances}/decor_carte.rda" |> glue()
repository_notc <- "{temp_dir}/notc" |> glue()
dir.create(repository_notc)
localr5 <- str_c(localdata, "/r5_base")
localr5car <- str_c(localdata, "/r5car")

## informations spécifique sur la ville
elevation_tif <- marseille_board  %>% pin_download("elevation_aix_marseille.tif") |> glue()
emp33km_file <- marseille_board  %>% pin_download("emp33km.qs") |> glue()
c200_file <- marseille_board  %>% pin_download("c200_17.qs") |> glue()
pbf_file <- "lr.pbf"
pbf_rep <- "{repository}/OSM/" |> glue()
dodgr_profiles <- "{localdata}/dodgr/dodgr_profiles.json" |> glue()
alternative_scenario <- "{localdata}/co2/filo_carbone_okok.gpkg" |> glue()
alternative_scenario2 <- "{localdata}/co2/filosofi_scenario_s4.csv" |> glue()

## informations qu'on peut garder car elles sont sur la France et donc 
# seront filtrées différemment une fois qu'on change la zone
densitescommunes <- marseille_board  %>% pin_download("grid.xlsx") |> glue() #le fichier Communes/grid.xslx qui était dans scratch
c200_stars <- marseille_board  %>% pin_download("c200_stars.rda") |> glue() #vient de data_villes
popactive_file <- marseille_board  %>% pin_download("base-cc-emploi-pop-active-2018.xlsx") |> glue()
classification_urbaine <- "{localdata}/autres/classification_urbaine.csv" |> glue()

## enquetes nationales
deploc_file <- "{mob2019}/k_deploc_public.csv" |> glue()
k_individu_file <- "{mob2019}/k_individu_public.csv" |> glue()
tcm_men_file <- "{mob2019}/tcm_men_public.csv" |> glue()
q_men_file <- "{mob2019}/q_menage_public.csv" |> glue()
tcm_ind_kish_file <- "{mob2019}/tcm_ind_kish_public.csv" |> glue()
enqmobpro <- marseille_board  %>% pin_download("mobpro2018.csv") |> glue()
#enqmobscol <- "{DVFdata}/sources/MOB/mobscol2018.csv" |> glue()

# c200ze_file <- "{DVFdata}/c200ze_lr.qs" |> glue()
# c200edu_file <- "{DVFdata}/c200edu_lr.qs" |> glue()
# c200edu_init_file  <- "{DVFdata}/c200edu_lr_init.qs" |> glue()

# 100 minutes de trajet en voiture max et
# 35km pour les probas (au delà proba_car=1)
seuil_temps_car <- 100
seuil_distance_proba <- 35000

save(list = unique(
  c(bl, 
    "alternative_scenario",
    "alternative_scenario2",
    "c200_file",
    # "c200ze_file",
    # "c200edu_file",
    # "c200edu_init_file",
    "decor_carte_file",
    "densitescommunes",
    "deploc_file",
    "distances_file",
    "distances_scol_file",
    # "DVFdata",
    "elevation_aix_marseille_tif",
    "emp33km_file", 
    "enqmobpro",
    "enqmobscol",
    "enqmobstarter_file",
    "home", 
    "idINS_emp_file",
    "idINS_scol_file",
    "k_individu_file",
    "kmnonpro_file",
    "kmscol_file",
    "kmscol_ppo_file",
    "kmpro_file", 
    "localdata", 
    "localr5",
    "meaps_file",
    "meaps_rep",
    "mob2019", 
    "mod_rep", 
    "newpredict_file",
    "nonproproba_file",
    "output_rep",
    "pbf_file",
    "pbf_rep",
    "popactive_file",
    "proproba_file",
    "q_men_file",
    "r5files_rep",
    "r5_output",
    "repository_distances",
    "repository_distances_scol",
    "repository_distances_emploi",
    "repository_notc", 
    "repository", 
    "scripts",
    "seuil_distance_proba",
    "seuil_temps_car",
    "tcm_ind_kish_file",
    "tcm_men_file",
    "temp_dir",
    "scot1.n",
    "scot2.n",
    "scot3.n",
    "scot4.n",
    "scot5.n",
    "scot_tot.n",
    "ville")),
  file = "baselayer.rda")