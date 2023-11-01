setwd("~/marseille")
# init ---------------
library(tidyverse)
library(tidytransit)
library(terra)
library(tictoc)
# remotes::install_github("OFCE/accesstars")
library(accesstars) 
library(tmap)
# pak::pak("xtimbeau/accessibility")
library(accessibility)
library(r3035)
library(glue)
library(stars)
library(data.table)
library(conflicted)
library(sitools)
library(arrow)
library(furrr)
library(ofce)
progressr::handlers(global = TRUE)
progressr::handlers("cli")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

# OSM en format silicate ------------
# c'est enregistré, donc on peut passer si c'est  déjà fait
osm_file <- glue("{mdir}/osm.qs")
if(!file.exists(osm_file)) {
  zone <- qs::qread(communes_ar_file) |> 
    filter(mobpro99) |> 
    summarize()
  osm <- download_osmsc(zone, elevation = TRUE, workers = 16)
  qs::qsave(osm, osm_file)
  rm(osm)
  gc()
}

dodgr_router <- routing_setup_dodgr(path = glue("{mdir}/dodgr/"), 
                                    osm = osm_file,
                                    mode = "CAR", 
                                    turn_penalty = TRUE,
                                    distances = TRUE,
                                    denivele = TRUE,
                                    n_threads = 16L,
                                    overwrite = FALSE,
                                    nofuture = TRUE)

# premier essai, infructueux pour le moment
# plan("multisession", workers = 4L)
# paires <- open_dataset(paires_mobpro_dataset)
# 
# unlink("{mdir}/temp_dodgr" |> glue(), recursive = TRUE)
# 
# communes <- paires |> distinct(COMMUNE) |> collect() |> pull()
# 
# paire <- paires |> 
#   filter(COMMUNE == communes[[1]]) |> 
#   collect()
# dodgr_pairs(od = paire |> slice(1:5000), routing = dodgr_router)
# 
# deuxième essai, (il y a un pb avec les index)

plan("multisession", workers = 4)

mobpro <- qs::qread(mobpro_file) |> 
  filter(mobpro95) |> 
  group_by(COMMUNE, DCLT, TRANS) |> 
  summarise(emp = sum(NB_in, na.rm=TRUE), .groups = "drop") |> 
  pivot_wider(names_from = TRANS, values_from = emp, values_fill = 0)

COMMUNEs <- mobpro |> distinct(COMMUNE) |> pull()
DCLTs <- mobpro |> distinct(DCLT) |> pull()

idINSes <- qs::qread(c200ze_file) |> 
  st_drop_geometry() |> 
  select(com=com22, idINS, scot, emp_resident, ind) |> 
  mutate(from = scot & (ind>0) & com%in%COMMUNEs,
         to = emp_resident>0 & com %in%DCLTs) |> 
  filter(from | to)

dgr_distances_by_com(idINSes, mobpro, dodgr_router, clusterize = TRUE)  

dgr_distances_by_paires(idINSes, mobpro, dodgr_router, chunk = 10000)  

dgr_distances_full(idINSes, dodgr_router, chunk = 5000000)  

dgr_distances_by_com(idINSes, mobpro |> filter(walk>0), dodgr_router, path = "dgr_walk")  

# tentative d'optimisation

com2com <- mobpro |>
  filter(bike>0) |> 
  select(COMMUNE, DCLT) |> 
  left_join(idINSes |> filter(from) |> count(com, name = "nfrom"), 
            by=c("COMMUNE"="com")) |> 
  left_join(idINSes |> filter(to) |> count(com, name = "nto"),
            by=c("DCLT"="com")) |> 
  mutate(n = nfrom*nto)

nDCLT <- com2com |> distinct(DCLT, nto) |> pull(nto, name = DCLT)
nCOMMUNE <- com2com |> distinct(COMMUNE, nfrom) |> pull(nfrom, name = COMMUNE)

dclts <- com2com |> 
  group_by(COMMUNE) |>
  summarize(dclt = list(DCLT), grids = list(nto), nfrom = first(nfrom)) |>
  mutate(l = map_int(dclt, length)) |>
  mutate( enplus = map(dclt, ~setdiff(.x,unlist(dclt[COMMUNE==13001]))),
          enmoins = map(dclt, ~setdiff(unlist(dclt[COMMUNE==13001]), .x)),
          union = map(dclt, ~unique(c(.x, unlist(dclt[COMMUNE==13001]))))) |> 
  mutate(n_union = map_int(union, ~sum(nDCLT[.x]) ) * (nfrom+(1-(COMMUNE==13001))*nfrom[COMMUNE==13001]),
         n_seul = map_int(dclt, ~sum(nDCLT[.x]) ) * nfrom,
         perte = 1-(n_seul+(1-(COMMUNE==13001))*n_seul[COMMUNE==13001])/n_union) |> 
  arrange(desc(perte))

dclts <- com2com |> group_by(COMMUNE) |> summarize(dclt = list(DCLT), grids = list(nto), nfrom = first(nfrom)) |> mutate(l = map_int(dclt, length)) |> mutate( enplus = map(dclt, ~setdiff(.x,unlist(dclt[COMMUNE==13001]))))

# distance pairique -----------
# la distance est définie comme d = (np_union - np_séparés)/(nfrom1 * nto2 + nfrom2 * nto1)

d_pairique <- function( com1, com2, data = com2com) {
  if(com1==com2)
    return(0)
  com1 <- as.character(com1)
  com2 <- as.character(com2)
  nDCLT <- data |> distinct(DCLT, nto) |> pull(nto, name = DCLT)
  nCOMMUNE <- data |> distinct(COMMUNE, nfrom) |> pull(nfrom, name = COMMUNE)
  n1 <- nCOMMUNE[[com1]]
  n2 <- nCOMMUNE[[com2]]
  com1dclt <- data |> filter(COMMUNE==com1) |> pull(DCLT)
  com2dclt <- data |> filter(COMMUNE==com2) |> pull(DCLT)
  nto1 <- sum(nDCLT[com1dclt])
  nto2 <- sum(nDCLT[com2dclt])
  np_sep <- n1 * nto1 + n2 * nto2
  np_union <- (n1+n2) * sum(nDCLT[unique(c(com1dclt, com2dclt))])
  np_cross <- n1 * nto2 + n2 * nto1
  return((np_union-np_sep)/ np_cross)
}

bench::mark(d_pairique(13001, 13002))
plan("multisession", workers = 16)
communes <- com2com |> distinct(COMMUNE) |> pull()
n <- length(communes)

mm <- matrix(0, nrow = n, ncol = n, dimnames = list(communes, communes))
for(i in 2:n)
  for(j in 1:(i-1))  
    mm[i,j] <- mm[j,i] <- d_pairique(communes[[i]], communes[[j]]) 
mm <- as.dist(mm)

clust <- factoextra::hcut(mm, method = "complete", k=4)
clusters <- hclust(mm, method = "complete")
ggdendro::ggdendrogram(clusters, rotate = TRUE)
best.cut <- NbClust::NbClust(diss=as.dist(mm), method="ward.D", distance=NULL, index="silhouette")
best.cut$Best.partition |> sort()

kk <- map_dfr(1:100, ~com2com |> 
                mutate(clust  = factoextra::hcut(as.dist(mm), method = "complete", k=.x)$cluster[COMMUNE]) |> 
                group_by(clust) |> 
                summarize(ncom = sum(nCOMMUNE[unique(COMMUNE)]) ,
                          ndclt = sum(nDCLT[unique(DCLT)]),
                          n = ncom*ndclt) |> 
                summarize( 
                  k = .x,
                  total_k = sum(n),
                  min_k = min(ncom*ndclt),
                  total = sum(nCOMMUNE)*sum(nDCLT),
                  ecart_temps = sum((n/total) * (pmin(total, 20e+6)/pmin(n, 20e+6))^0.5), 
                  ecart = total_k/total))
ggplot(kk)+geom_point(aes(x=k, y=ecart_temps))+geom_point(aes(x=k, y=ecart), col="orange")

clusterize_com2com <- function(data, seuil = 10e+6L, method = "complete") {
  nDCLT <- data |> distinct(DCLT, nto) |> pull(nto, name = DCLT)
  nCOMMUNE <- data |> distinct(COMMUNE, nfrom) |> pull(nfrom, name = COMMUNE)
  d_pairique <- function( com1, com2) {
    if(com1==com2)
      return(0)
    com1 <- as.character(com1)
    com2 <- as.character(com2)
    n1 <- nCOMMUNE[[com1]]
    n2 <- nCOMMUNE[[com2]]
    com1dclt <- data |> filter(COMMUNE==com1) |> pull(DCLT)
    com2dclt <- data |> filter(COMMUNE==com2) |> pull(DCLT)
    nto1 <- sum(nDCLT[com1dclt])
    nto2 <- sum(nDCLT[com2dclt])
    np_sep <- n1 * nto1 + n2 * nto2
    np_union <- (n1+n2) * sum(nDCLT[unique(c(com1dclt, com2dclt))])
    np_cross <- n1 * nto2 + n2 * nto1
    return((np_union-np_sep)/ np_cross)
  }
  
  communes <- data |> distinct(COMMUNE) |> pull()
  n <- length(communes)
  
  mm <- matrix(0, nrow = n, ncol = n, dimnames = list(communes, communes))
  for(i in 2:n)
    for(j in 1:(i-1))  
      mm[i,j] <- mm[j,i] <- d_pairique(communes[[i]], communes[[j]]) 
  mm <- as.dist(mm)
  
  kk <- map_dfr(1:(n-1), ~data |> 
                  mutate(clust  = factoextra::hcut(
                    mm,
                    method = method, 
                    k=.x)$cluster[COMMUNE]) |> 
                  group_by(clust) |> 
                  summarize(ncom = sum(nCOMMUNE[unique(COMMUNE)]) ,
                            ndclt = sum(nDCLT[unique(DCLT)]),
                            n = ncom*ndclt) |> 
                  summarize( 
                    k = .x,
                    total_k = sum(n),
                    min_k = min(ncom*ndclt),
                    total = sum(nCOMMUNE)*sum(nDCLT),
                    ecart_temps = sum((n/total) * (pmin(total, seuil)/pmin(n, seuil))^0.5), 
                    ecart = total_k/total))
  k <- kk |> filter(ecart_temps==min(ecart_temps)) |> pull(k)
  list(cluster = factoextra::hcut(mm, method = method, k=k)$cluster,
       meta = as.list(kk |> filter(ecart_temps==min(ecart_temps))))
}
