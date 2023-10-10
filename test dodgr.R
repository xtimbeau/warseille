library(tidyverse)
library(sf)
library(furrr)
library(qs)
library(dodgr)
library(accessibility)
library(lobstr)

list.files("accessibility", full.names = TRUE) %>% walk(source)


# box contient les limites des données OSM que l'on veut capturer
# typiquement la zone d'étude un peu augmentée

com17 <- com17 |> rename(CODGEO=insee)
communes <- communes |> inner_join(com17, by='CODGEO')
communes <- st_as_sf(communes)

box <- communes |> 
  filter(REG ==93) |> 
  st_union() |> 
  st_buffer(5000) |> 
  st_transform(4326) |>
  st_bbox()

# le téélpchargement d'OSM via osmdata en silicate
# ca peut être un peu long (20m pour Paris)
if(FALSE) {
  osm <- download_osmsc(box, workers = 16)
  qs::qsave(osm, "~/files/data/osm.qs")
  dir.create("~/files/dodgr/paca", recursive = TRUE)
  file.copy("~/files/data/osm.qs", to = "~/files/dodgr/paca/paca.scosm", overwrite = TRUE)
}


# on lance le calcul du réseau, 
# le fichiers sont mis en cache, 
# pour écraser le cache, il faut soit overwrite=TRUE
# soit effacer les fichiers mis en cache

dodgr_router <- routing_setup_dodgr("~/files/dodgr/paca" |> glue(), 
                                    mode = "CAR", 
                                    turn_penalty = TRUE,
                                    distances = TRUE,
                                    n_threads = 4L,
                                    overwrite = FALSE)
# On définit les origines
c200 <- c200ze |>
  st_centroid() |> 
  st_transform(4326) |> 
  select(idINS, ind)

# les destinations (ou opportunités)
opp <- c200 |> 
  filter(ind>500)

# il faut faire attention à la consommation de mémoire
# ca dépend du nombre de process, de la taille du réseau et d'autres choses
# ttm_out=TRUE fait que la sortie est sous forme de table de distances
# ttm idINS le transforme en un format à plat 
# (attention on perd les id originaux de dodgr)
plan("multisession", workers = 4)
iso_transit_dt <- iso_accessibilite(quoi = opp, 
                                    ou = c200, 
                                    resolution = 200,
                                    tmax = 120, 
                                    pdt = 1,
                                    dir = "temp_dodgr",
                                    routing = dodgr_router,
                                    ttm_out = TRUE,
                                    future=TRUE)

ttt <- accessibility::ttm_idINS(iso_transit_dt)

 
# to/from comp --------
# le snap se fait sur les noeuds (et pas sur les arrêtes)
# on compare ici les deux
 
comp <- left_join(
  iso_transit_dt$fromId,
  dodgr_router$graph |> as_tibble() |>  select(idalt = .vx0, lonalt = .vx0_x, latalt =.vx0_y), by = "idalt") |> 
  drop_na(idalt, lonalt, latalt) |> 
  mutate(
    pt = map2(lon, lat, ~st_point(c(..1, ..2))),
    pt_alt = map2(lonalt, latalt, ~st_point(c(..1, ..2))),
    line = pmap(list(lon,lat,lonalt, latalt), ~st_linestring(rbind(c(..1, ..2),c(..3, ..4))))) 

tmap_mode("view")
tm_shape(comp |> st_as_sf(sf_column_name = "line", crs = 4326))+tm_lines()+
  tm_shape(comp |> st_as_sf(sf_column_name = "pt", crs = 4326))+tm_dots(size=1)
  
  
 