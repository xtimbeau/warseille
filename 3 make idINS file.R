

library(tidyverse)
library(accesstars)
library(tmap)
library(pak)
# pak::pak('xtimbeau/accessibility')
library(accessibility)
library(r3035)
library(glue)
library(stars)
library(data.table)
library(conflicted)
library(arrow)
conflicted::conflict_prefer("filter", "dplyr", quiet=TRUE)
conflicted::conflict_prefer("select", "dplyr", quiet=TRUE)
conflicted::conflict_prefer("first", "dplyr", quiet=TRUE)

progressr::handlers(global = TRUE)
progressr::handlers(progressr::handler_progress(format = ":bar :percent :eta", width = 80))
arrow::set_cpu_count(8)

## globals --------------------
load("baselayer.rda")

modes <- names(r5_output)
files <- r5_output

# data <- map(files, ~{
#   setDT(arrow::read_parquet("{repository_distances_emploi}/{.x}" |> glue(),
#                             col_select = c("fromidINS", "toidINS")))
# })

c200ze <- qs::qread(c200ze_file)
# scot_tot.epci <- com2021epci |> pull(INSEE_COM)

paires <- open_dataset(paires_mobpro_dataset)
iterator <- paires |> count(COMMUNE, DCLT) |> collect()


iterator <- iterator |> select(COMMUNE, DCLT) |> mutate(COMMUNE=as.character(COMMUNE), DCLT=as.character(DCLT))
iterator <- iterator |> inner_join(c200ze |> select(idINS,com22), by=c('COMMUNE'='com22')) |> rename(fromidINS=idINS)
iterator <- iterator |> inner_join(c200ze |> select(idINS,com22), by=c('DCLT'='com22')) |> rename(toidINS=idINS)

# fromidINSs <- unique(iterator |> select(fromidINS)) |> pull(fromidINS)
# toidINSs <- unique(iterator |> select(toidINS)) |> pull(toidINS)


# On calcule une distance euclidienne et on crée un id pour les paires
# si on prend les données c200, on a presque 213 millions de paires
euc <- iterator |> select(fromidINS, toidINS)
setDT(euc)
rm(iterator)
euc[, `:=`(euc = as.integer(idINS2dist(fromidINS, toidINS)),
           id = 1:.N)]
euc[ , `:=`(fromidINS = factor(fromidINS),
            toidINS = factor(toidINS))]
setkey(euc, fromidINS, toidINS)

# # on ajoute quelques infos 
# 
# iristofrom <- unique(c(fromidINSs, toidINSs)) |> 
#   idINS2point() |> 
#   as_tibble() |> 
#   mutate(idINS=idINS3035(X,Y)) |> 
#   st_as_sf(coords=c("X","Y"), crs=3035)
# iris18 <- qs::qread('~/files/iris18.qs')
# iris18 <- iris18 |> mutate(iris_area=st_area(geometry))
# irises <- st_join(iristofrom, iris18 |> select(CODE_IRIS, iris_area), join=st_within)
# c200ze <- qs::qread("{c200ze_file}" |> glue())
# 
# irises <- merge(irises, 
#                 c200ze |> st_drop_geometry() |> select(ind, idINS),
#                 by="idINS",
#                 all.x=TRUE)
# 
# nas <-iris18 |> 
#   slice(st_nearest_feature(irises |> filter(is.na(CODE_IRIS)), iris18)) |>
#   select(CODE_IRIS, iris_area) |> 
#   st_drop_geometry() |> 
#   mutate(idINS = irises |> filter(is.na(CODE_IRIS)) |> pull(idINS))
# irises <- bind_rows(irises |> filter(!is.na(CODE_IRIS)) |> st_drop_geometry(), nas)
# irises <- irises |>
#   group_by(CODE_IRIS) |>
#   mutate(densite = as.numeric(sum(ind, na.rm=TRUE)/first(iris_area)*1000*1000)) |>
#   ungroup() |> 
#   select(-iris_area, -ind)
# 
# euc <- euc |> 
#   merge(irises |> rename(fromIris=CODE_IRIS, fromdensite=densite), 
#         by.x = "fromidINS", by.y = "idINS", 
#         all.x=TRUE, all.y=FALSE) |> 
#   merge(irises |> rename(toIris=CODE_IRIS,todensite=densite),
#         by.x = "toidINS", by.y = "idINS",
#         all.x=TRUE, all.y=FALSE)
# 
# euc[ , `:=`(
#   # fromidINS = factor(fromidINS),
#   # toidINS = factor(toidINS),
#   fromIris = factor(fromIris),
#   toIris = factor(toIris))]

gc()
c200 <- as_tibble(c200ze) |> 
  select(idINS, com22, scot, emp) |> 
  setDT()

euc <- euc |> 
  merge(c200[, .(fromidINS = factor(idINS), COMMUNE = factor(com22), scot)], 
        by = "fromidINS", 
        all.x = TRUE) |> 
  merge(c200[, .(toidINS = idINS, DCLT = factor(com22), emp=!is.na(emp)&emp>0)], 
        by = "toidINS",
        all.x=TRUE)
euc[ , `:=`(COMMUNE = factor(COMMUNE),
            DCLT = factor(DCLT),
            fromidINS = factor(fromidINS),
            toidINS = factor(toidINS),
            emp = emp) ]

setkey(euc, id)
gc()

# expérimental
# arrow::write_dataset(euc, "/scratch/distances/idINS_set", partitioning = "COMMUNE")

idINS_emp_file <- '/space_mounts/data/marseille/distances/src/idINS.parquet' |> glue()
arrow::write_parquet(euc, idINS_emp_file)
