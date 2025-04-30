library(tidyverse)
library(glue)
library(sf)

curl::curl_download(
  "https://www.insee.fr/fr/statistiques/fichier/2114627/grille_densite_niveau_detaille_2022.xlsx",
  destfile = "/tmp/zones_dens.xlsx")

dens <- readxl::read_xlsx("/tmp/zones_dens.xlsx") |> 
  rename(code = 1, dens = 3) |> 
  select(code, dens)   

communes <- ofce::bd_read("zones_res") |> 
  mutate(code = ifelse(str_detect(CODE_COM, "^132"), 13056, CODE_COM)) |> 
  left_join(dens, by = "code" ) |> 
  mutate(tooltip = glue("<b>{NOM_ZF}</b><br>Commune : {CODE_COM}")) |> 
  st_transform(4326) |> 
  st_simplify(dTolerance = 100, preserveTopology = FALSE)

coms <- ofce::bd_read("communes") |> filter(SIREN_EPCI == SIREN_EPCI[INSEE_COM=="13101"]) |> st_union()
# un commentaire ici encore et encore et encore
return(list(communes=communes, coms = coms))