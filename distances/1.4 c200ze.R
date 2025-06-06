# on fait un c200 local, correctement renseigné.
# il est la réunion de c200 individus, plus des carreaux flores, sur l'étendue zone_emploi
library(pins)
library(tidyverse)
library(stars)
library(sf)
library(glue)
library(conflicted)
library(data.table)
library(r3035)
library(qs)
library(arrow)
  conflict_prefer_all("dplyr", quiet=TRUE)
source("mglobals.r")

bl <- load("baselayer.rda")

c200 <- qread(c200_file) 

# version 2017

mobpro95 <- qs::qread(mobpro_file)

communes <- mobpro95 |> filter(mobpro95) |>  distinct(COMMUNE) |> pull()
dclts <- mobpro95 |> filter(mobpro95) |> distinct(DCLT) |> pull()
ttes_com <- unique(c(dclts, communes))
full_iris <- qread(iris_file)
iris <- full_iris |> 
  filter(DEPCOM %in% ttes_com) |> 
  mutate(id = 1:n())

com_ze <- iris |> 
  rename(COM=DEPCOM) |> 
  group_by(COM) |>
  st_drop_geometry() |> 
  summarize(DEP = first(DEP)) |>
  rename(idcom=COM)

empze <- qs::qread(emp_pred_file) |> 
  rename(emp = emp_pred,
         emp_resident = emp_pred_scot) |> 
  mutate(geometry = r3035::sidINS2square(idINS)) |> 
  st_as_sf(crs=3035)

irises <- sf::st_intersects(empze, iris) 
names(irises) <- 1:length(irises)
l_irises  <- map_int(irises, length)
nas <- sf::st_nearest_feature(empze |> filter(l_irises==0), iris)
irises[l_irises==0] <- nas

l2 <- st_join(
  empze |> filter(l_irises>=2), 
  iris |> select(id), largest=TRUE)

irises[l_irises>=2] <- l2 |> pull(id)
flat_irises <- purrr::list_c(irises)

act_mobpro <- qs::qread(mobpro_file) |> 
  filter(COMMUNE %in% communes) |> 
  group_by(COMMUNE) |> 
  mutate(mobpro95 = replace_na(mobpro95, FALSE)) |> 
  summarize(act_mobpro.tot = sum(NB),
            fuite_mobpro.tot = sum(NB)-sum(NB[mobpro95==TRUE], na.rm=TRUE))

# on ajoute les aménités
c200a <- open_dataset(amenites_file) |> 
  collect() |> 
  mutate(idINS = r3035::expand_idINS(idINS)) |> 
  r3035::idINS2sf() |> 
  st_join(y  = full_iris, join = st_intersects)
  
c200a_orp <- c200a |> filter(is.na(CODE_IRIS)) |> 
  select(idINS, starts_with("surf")) |> 
  st_join(full_iris, join = st_nearest_feature)

c200a <- bind_rows(c200a |> filter(!is.na(CODE_IRIS)),
                   c200a_orp) |> 
  st_drop_geometry() |> 
  pivot_longer(cols = starts_with("surf"), names_to = "type", values_to = "surf") |> 
  mutate(type = stringr::str_remove(type, "surf_eq_")) |> 
  filter(surf>0) |> 
  group_by(idINS) |> 
  summarize(across(c(CODE_IRIS, DEPCOM), first),
            amenite = str_c(type, collapse= "_")) |> 
  transmute(idINS = r3035::contract_idINS(idINS), 
            IRIS = CODE_IRIS, com = DEPCOM, com22 = DEPCOM, 
            dep = stringr::str_sub(DEPCOM, 1, 2),
            amenite)

c200i <- c200 |> 
  semi_join(com_ze, by=c("com22"="idcom")) |> 
  mutate(IRIS = CODE_IRIS, 
         com=com22) |> 
  left_join(act_mobpro, by = c("com22"= "COMMUNE")) |> 
  group_by(com22) |> 
  mutate(
    act_mobpro = ind / sum(ind, na.rm=TRUE) * act_mobpro.tot,
    fuite_mobpro = ind / sum(ind, na.rm=TRUE) * fuite_mobpro.tot) |> 
  ungroup() |> 
  select(-c(act_mobpro.tot, fuite_mobpro.tot), -CODE_IRIS)

c200e <- empze |>
  transmute(
    idINS, 
    dep=iris$DEP[flat_irises],
    com=iris$DEPCOM[flat_irises], # on prend les iris pour être cohérent, un carreau peut être sur plusieurs communes
    IRIS = iris$CODE_IRIS[flat_irises],
    emp, emp_resident) 

c200ze <- c200i |>
  st_drop_geometry() |> 
  full_join(c200e |> st_drop_geometry() |> select(idINS, emp, emp_resident, IRIS, com, dep),
                    by = "idINS", suffix = c("",".e")) |> 
  full_join(c200a, by = "idINS", suffix = c("", ".a")) |> 
  mutate(
    dep = if_else(is.na(dep), if_else(is.na(dep.e), dep.a, dep.e), dep),
    com = if_else(is.na(com), if_else(is.na(com.e), com.a, com.e), com),
    IRIS = if_else(is.na(IRIS), if_else(is.na(IRIS.e), IRIS.a, IRIS.e), IRIS),
    across(c(emp, emp_resident, ind, men, adultes, ind_18_64, ind_snv, act_mobpro),~replace_na(.x, 0)),
    amenite = replace_na(amenite, "") ) |>
  select(-ends_with(".e"), -ends_with(".a")) |> 
  mutate(geometry = r3035::sidINS2square(idINS)) |> 
  st_as_sf(crs=3035) 

c200ze <- c200ze |> 
  mutate(ze = com %in% dclts,
         scot = com %in% communes)

# on ajoute les géographies 17 à 22

# com17 <- sf::st_read(com2017_shp) |> 
#   sf::st_transform(3035)
# 
# com21 <- sf::st_read(com2021_shp) |> 
#   sf::st_transform(3035)
archive::archive_extract(
  "https://www.insee.fr/fr/statistiques/fichier/5395838/base-ccx-emploi-pop-active-2018.zip",
  "/tmp")
popact <- readxl::read_xlsx(
  "/tmp/base-cc-emploi-pop-active-2018.xlsx" |> glue(),
  sheet = "COM_2018", skip=5) |> 
  transmute(com21=CODGEO,
            tact1564 = P18_ACT1564/P18_POP1564,
            tactocc1564 = P18_ACTOCC1564/P18_POP1564)

c200ze <- c200ze |> 
  left_join(popact, by=c("com"="com21"))
qs::qsave(c200ze, file=c200ze_file)
bd_write(c200ze)
