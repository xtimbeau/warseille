# on fait un c200 local, correctement renseigné.
# il est la réunion de c200 individus, plus des carreaux flores, sur l'étendue zone_emploi
library(pins)
library(tidyverse)
library(stars)
library(sf)
library(glue)
library(conflicted)
library(data.table)
conflict_prefer("select", "dplyr")
conflict_prefer("filter", "dplyr")

bl <- load("baselayer.rda")

emp33km <- qs::qread(emp33km_file) |> 
  as_tibble() |> 
  filter(emp_pred>0) |> 
  st_as_sf(coords=c("x", "y"), crs=3035) 

c200.src <- setDT(c200)
# iris18 <- read_xlsx("~/files/iris18.xlsx")
c200i <- c200.src[IRIS %in% iris[ st_intersects(iris, zone_emploi, sparse=FALSE), ]$IRIS,]
c200i[, geometry := r3035::idINS2square(idINS) ]

c200i <- st_as_sf(c200i, crs=3035) 

irises <- sf::st_intersects(emp33km, iris) 
irises <- map_int(irises , ~ifelse(length(.x)==0, NA, .x[[1]]))
nas <- sf::st_nearest_feature(emp33km[is.na(irises),], iris)
irises[is.na(irises)] <- nas

c200e <- emp33km |>
  as_tibble() |> 
  transmute(
    idINS=r3035::idINS3035(st_coordinates(emp33km), resolution = 200), 
    dep=iris$DEP[irises],
    com=iris$DEPCOM[irises], # on prend les iris pour être cohérent, un carreau peut être sur plusieurs communes
    CODE_IRIS = iris$CODE_IRIS[irises],
    emp = emp_pred
  ) 

c200ze <- full_join(c200i |> as_tibble(), c200e |> as_tibble(), by = "idINS", suffix = c("",".e")) |> 
  transmute(
    idINS,
    CODE_IRIS = if_else(is.na(CODE_IRIS), CODE_IRIS.e, CODE_IRIS),
    dep = if_else(is.na(dep), dep.e, dep),
    emp, ind, men, adultes, enfants, ind_18_64, ind_snv
  ) |>
  distinct(idINS, .keep_all = TRUE) |> 
  mutate(geometry = r3035::idINS2square(idINS)) |> 
  st_as_sf(crs=3035) 

# on ajoute les géographies 17 à 22

com17 <- sf::st_read(com2017_shp) |> 
  sf::st_transform(3035)

com21 <- sf::st_read(com2021_shp) |> 
  sf::st_transform(3035)

c200ze <- st_join(c200ze, com17 |> select(com17 = insee), join=st_intersects, largest=TRUE) |> 
  st_join(com21 |> select(com21 = insee), join=st_intersects, largest=TRUE) 
c200ze <- c200ze |> mutate(scot = com17%in%scot3.epci)

popact <- readxl::read_xlsx("{popactive_file}" |> glue(),sheet = "COM_2018", skip=5) |> 
  transmute(com21=CODGEO,
            tact1564 = P18_ACT1564/P18_POP1564)

c200ze <- c200ze |> 
  left_join(popact, by="com21")

qs::qsave(c200ze, file=c200ze_file)

save(list = c(bl, "c200ze_file"), file="baselayer.rda")

marseille_board %>% pin_upload(system.file("c200ze_file"))
