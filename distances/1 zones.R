# init ---------------
library(tidyverse)
library(glue)
library(conflicted)
library(rlang)
library(glue)
library(stringr)
library(pins)
library(AzureStor) 

progressr::handlers(global = TRUE)
progressr::handlers(progressr::handler_progress(format = ":bar :percent :eta", width = 80))

bl <- NULL
if(fs::file_exists("baselayer.rda")) bl <- load("baselayer.rda")

# globals -----------------------------------------------------------------
ville <- "Marseille"
epci.metropole <- "200054807"

scot1.n <- c("Allauch",	"Carry-le-Rouet","Cassis", "Ceyreste",
             "Châteauneuf-les-Martigues","La Ciotat","Ensuès-la-Redonne",
             "Gémenos","Gignac-la-Nerthe","Marignane","Marseille",
             "Plan-de-Cuques","Roquefort-la-Bédoule","Le Rove","Saint-Victoret",
             "Sausset-les-Pins","Septèmes-les-Vallons","Carnoux-en-Provence")
scot2.n <- c("Aix-en-Provence","Beaurecueil","Bouc-Bel-Air","Cabriès",
             "Châteauneuf-le-Rouge","Éguilles","Fuveau","Jouques","Lambesc",
             "Meyrargues","Meyreuil","Mimet","Les Pennes-Mirabeau","Peynier",
             "Peyrolles-en-Provence","Puyloubier","Le Puy-Sainte-Réparade",
             "Rognes","La Roque-d'Anthéron","Rousset","Saint-Antonin-sur-Bayon",
             "Saint-Cannat","Saint-Estève-Janson","Saint-Marc-Jaumegarde",
             "Saint-Paul-lès-Durance","Simiane-Collongue","Le Tholonet",
             "Trets","Vauvenargues","Venelles","Ventabren","Vitrolles","Coudoux","Pertuis")
scot3.n <- c("Alleins","Aurons","La Barben","Berre-l'Étang","Charleval","Eyguières",
             "La Fare-les-Oliviers","Lamanon","Lançon-Provence","Mallemort",
             "Pélissanne","Rognac","Saint-Chamas","Salon-de-Provence","Sénas",
             "Velaux","Vernègues")
scot4.n <- c("Aubagne","Auriol","Belcodène","La Bouilladisse","Cadolive",
             "Cuges-les-Pins","La Destrousse","Gréasque","La Penne-sur-Huveaune",
             "Peypin","Roquevaire","Saint-Savournin","Saint-Zacharie")
scot5.n <- c("Arles","Aureille","Barbentane","Les Baux-de-Provence",
             "Boulbon","Cabannes","Châteaurenard","Eygalières","Eyragues",
             "Fontvieille","Graveson","Maillane","Mas-Blanc-des-Alpilles",
             "Maussane-les-Alpilles","Saint-Pierre-de-Mézoargues","Mollégès",
             "Mouriès","Noves","Paradou","Rognonas","Saint-Andiol",
             "Saint-Étienne-du-Grès","Saintes-Maries-de-la-Mer","Saint-Martin-de-Crau",
             "Saint-Rémy-de-Provence","Tarascon","Verquières")
scot5.n <- c("Cornillon-Confoux","Fos-sur-Mer","Grans","Istres",
             "Martigues","Miramas","Port-de-Bouc","Port-Saint-Louis-du-Rhône",
             "Saint-Mitre-les-Remparts")

scot_tot.n <- c("Allauch",	"Carry-le-Rouet","Cassis", "Ceyreste",
                "Châteauneuf-les-Martigues","La Ciotat","Ensuès-la-Redonne",
                "Gémenos","Gignac-la-Nerthe","Marignane","Marseille",
                "Plan-de-Cuques","Roquefort-la-Bédoule","Le Rove","Saint-Victoret",
                "Sausset-les-Pins","Septèmes-les-Vallons","Carnoux-en-Provence",
                "Aix-en-Provence","Beaurecueil","Bouc-Bel-Air","Cabriès",
                "Châteauneuf-le-Rouge","Éguilles","Fuveau","Jouques","Lambesc",
                "Meyrargues","Meyreuil","Mimet","Les Pennes-Mirabeau","Peynier",
                "Peyrolles-en-Provence","Puyloubier","Le Puy-Sainte-Réparade",
                "Rognes","La Roque-d'Anthéron","Rousset","Saint-Antonin-sur-Bayon",
                "Saint-Cannat","Saint-Estève-Janson","Saint-Marc-Jaumegarde",
                "Saint-Paul-lès-Durance","Simiane-Collongue","Le Tholonet","Trets",
                "Vauvenargues","Venelles","Ventabren","Vitrolles","Coudoux","Pertuis",
                "Alleins","Aurons","La Barben","Berre-l'Étang","Charleval","Eyguières",
                "La Fare-les-Oliviers","Lamanon","Lançon-Provence","Mallemort",
                "Pélissanne","Rognac","Saint-Chamas","Salon-de-Provence","Sénas",
                "Velaux","Vernègues","Aubagne","Auriol","Belcodène","La Bouilladisse",
                "Cadolive","Cuges-les-Pins","La Destrousse","Gréasque",
                "La Penne-sur-Huveaune","Peypin","Roquevaire","Saint-Savournin",
                "Saint-Zacharie","Arles","Aureille","Barbentane","Les Baux-de-Provence",
                "Boulbon","Cabannes","Châteaurenard","Eygalières","Eyragues","Fontvieille",
                "Graveson","Maillane","Mas-Blanc-des-Alpilles","Maussane-les-Alpilles",
                "Saint-Pierre-de-Mézoargues","Mollégès","Mouriès","Noves","Paradou",
                "Rognonas","Saint-Andiol","Saint-Étienne-du-Grès","Saintes-Maries-de-la-Mer"
                ,"Saint-Martin-de-Crau","Saint-Rémy-de-Provence","Tarascon","Verquières"
                ,"Cornillon-Confoux","Fos-sur-Mer","Grans","Istres","Martigues","Miramas"
                ,"Port-de-Bouc","Port-Saint-Louis-du-Rhône","Saint-Mitre-les-Remparts")

marseille_board <- pins::board_azure(
  AzureStor::storage_container(Sys.getenv("azure_url"), sas = Sys.getenv("azure_sas")))

#à faire juste une fois
# marseille_board %>% pin_upload("Mobilités des Personnes 2019")
# marseille_board %>% pin_upload("emp33km.qs")
# marseille_board %>% pin_upload("c200_17.qs")
# marseille_board %>% pin_upload("grid.xlsx")
# marseille_board %>% pin_upload("c200_stars.rda")
# marseille_board %>% pin_upload("base-cc-emploi-pop-active-2018.xlsx")
# marseille_board %>% pin_upload("~/files/marseille_parcs_jardins_2018.csv") 
# marseille_board %>% pin_upload("gtfs_marseille_zip.zip") 
# marseille_board %>% pin_upload("~/files/iris18.7z") 
# marseille_board %>% pin_upload("~/files/reference_IRIS_geo2018.xls") 
# marseille_board %>% pin_upload("~/files/carreaux.zip") 
# marseille_board %>% pin_upload("~/files/iris18.xlsx") 
# #marseille_board %>% pin_upload("~/files/c200.xslx") 
# marseille_board %>% pin_upload("~/files/communes_2017.zip") 
# marseille_board %>% pin_upload("~/files/communes_2021.zip")
# marseille_board %>% pin_upload("~/files/Intercommunalite_Metropole_au_01-01-2017.xls")

# fichiers importants
mdir <- "/space_mounts/data/marseille/"
localdata <- "~/files/localr5/" |> glue()
scripts <- "~/marseille"
mob2019 <- "{mdir}/mob2019/" |> glue()
home <- "~/marseille"
temp_dir <- "/tmp"
r5files_rep <- "{mdir}/distances/src/" |> glue()
newpredict_file <- "annexes/newpredict.r"
enqmobstarter_file <- "enqmob_starter_V2.R"
mod_rep <- "{mdir}/mod" |> glue()
output_rep <- "{mdir}/output/" |> glue()
dir.create(output_rep)
dir.create(mod_rep)

# distances, probas et tutti quanti
r5files_rep <- "{mdir}/distances/router" |> glue()
r5_output <- set_names(
  c("bike.parquet", "cardgr.parquet", "transit.parquet", "walk.parquet"),
  c("bike",         "car",          "transit",         "walk"))
dir_dist <- "{mdir}/distances" |> glue()
idINS_file <- "{mdir}/idINS.parquet" |> glue()
# idINS_scol_file <- "{repository_distances_scol}/idINS.parquet" |> glue()
distances_file <- "{dir_dist}/distances.parquet" |> glue()
dist_dts <- "{dir_dist}/src/distances_dataset" |> glue()
proproba_file <- "{dir_dist}/proba_pro.parquet" |> glue()
nonproproba_file <- "{dir_dist}/proba_nonpro.parquet"
kmpro_file <- "{dir_dist}/km_pro.parquet"
kmnonpro_file <- "{dir_dist}/km_nonpro.parquet"
trajets_pro_file <- "{dir_dist}/trajets_pro.parquet" |> glue()
meaps_rep <- "{dir_dist}/meaps" |> glue()
dir.create(meaps_rep)
meaps_file <- "{meaps_rep}/meaps_est.rda" |> glue()
decor_carte_file <- "{mdir}/decor_carte.rda" |> glue()
mobilites_file <- "{mdir}/mobilites.qs" |> glue()
repository_notc <- "{temp_dir}/notc" |> glue()
dir.create(repository_notc)
localr5 <- str_c(localdata, "/r5_base")
localr5car <- str_c(localdata, "/r5car")

time_matrix <- "{dir_dist}/time_matrix.qs" |> glue()
rank_matrix <- "{dir_dist}/rank_matrix.qs" |> glue()
delta_dts <- "{mdir}/delta_iris" |> glue()
# résultats intermédiaires
mobpro_file <- "{mdir}/mobpro.qs" |> glue()
empetact <- "{mdir}/empetact.qs" |> glue()
paires_mobpro_dataset <- "{mdir}/paires_mobpro" |> glue()

## informations spécifique sur la ville
elevation <- "{localr5}/elev_aix_marseille.tif" |> glue()
empze_file <- "{mdir}/empze.qs" |> glue()
c200_file <- "{mdir}/c200_17.qs" |> glue()
c200ze_file <- "{mdir}/c200ze.qs" |> glue()
pbf_file <- "lr.pbf"
pbf_rep <- "{mdir}/OSM/" |> glue()
communes_file <- "{mdir}/communes.qs" |> glue()
communes_ref_file <- "{mdir}/communes_ref.qs" |> glue()
communes_ar_file <- "{mdir}/communes_ar.qs" |> glue()
communes_mb99_file <- "{mdir}/communes_mb99.qs" |> glue()
emp_pred_file <- "{mdir}/emp_pred.qs" |> glue()
# dodgr_profiles <- "{localdata}/dodgr/dodgr_profiles.json" |> glue()
# alternative_scenario <- "{localdata}/co2/filo_carbone_okok.gpkg" |> glue()
# alternative_scenario2 <- "{localdata}/co2/filosofi_scenario_s4.csv" |> glue()


## informations qu'on peut garder car elles sont sur la France et donc 
# seront filtrées différemment une fois qu'on change la zone
# densitescommunes <- marseille_board  %>% pin_download("grid.xlsx") |> glue() #le fichier Communes/grid.xslx qui était dans scratch
# c200_stars <- marseille_board  %>% pin_download("c200_stars.rda") |> glue() #vient de data_villes
# popactive_file <- marseille_board  %>% pin_download("base-cc-emploi-pop-active-2018.xlsx") |> glue()
classification_urbaine <- "{localdata}/autres/classification_urbaine.csv" |> glue()
iris_file <- glue("{mdir}/iris.qs")
com2021_file <- glue("{mdir}/com2021.qs")
com2017_file <- glue("{mdir}/com2017.qs")

## enquetes nationales
deploc_file <- "{mob2019}/k_deploc_public.csv" |> glue()
k_individu_file <- "{mob2019}/k_individu_public.csv" |> glue()
tcm_men_file <- "{mob2019}/tcm_men_public.csv" |> glue()
q_men_file <- "{mob2019}/q_menage_public.csv" |> glue()
tcm_ind_kish_file <- "{mob2019}/tcm_ind_kish_public.csv" |> glue()
enqmobpro <- "{mdir}/mobpro2018.csv" |> glue()
#enqmobscol <- "{DVFdata}/sources/MOB/mobscol2018.csv" |> glue()

# c200ze_file <- marseille_board  %>% pin_download(c200ze.qs) |> glue()

# 100 minutes de trajet en voiture max et
# 35km pour les probas (au delà proba_car=1)
seuil_temps_car <- 120
seuil_distance_proba <- 35000

save(list = unique(
  c(bl, 
    "c200_file",
    "c200ze_file",
    "com2017_file",
    "com2021_file",
    "communes_file",
    "communes_ar_file",
    "communes_ref_file",
    "communes_mb99_file",
    "decor_carte_file",
    "delta_dts",
    "densitescommunes",
    "deploc_file",
    "dir_dist",
    "distances_file",
    "dist_dts",
    "distances_scol_file",
    "elevation",
    "emp_pred_file",
    "empze_file", 
    "enqmobpro",
    "enqmobscol",
    "enqmobstarter_file",
    "epci.metropole",
    "idINS_file",
    "iris_file",
    "k_individu_file",
    "kmnonpro_file",
    "kmscol_file",
    "kmscol_ppo_file",
    "kmpro_file", 
    "localdata", 
    "localr5",
    "marseille_board",
    "mdir",
    "meaps_file",
    "meaps_rep",
    "mob2019", 
    "mod_rep",
    "mobpro_file",
    "newpredict_file",
    "nonproproba_file",
    "output_rep",
    "paires_mobpro_dataset",
    "pbf_file",
    "pbf_rep",
    "popactive_file",
    "proproba_file",
    "q_men_file",
    "r5files_rep",
    "r5_output",
    "rank_matrix",
    "repository_distances",
    "repository_distances_scol",
    "repository_distances_emploi",
    "repository_notc", 
    "repository", 
    "scripts",
    "seuil_distance_proba",
    "seuil_temps_car",
    "scot1.n",
    "scot2.n",
    "scot3.n",
    "scot4.n",
    "scot5.n",
    "scot_tot.n",
    "tcm_ind_kish_file",
    "tcm_men_file",
    "temp_dir",
    "time_matrix",
    "ville")),
  file = "baselayer.rda")