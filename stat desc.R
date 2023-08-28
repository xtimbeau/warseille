#pour montrer les flux 

remotes::install_github("FlowmapBlue/flowmapblue.R")
library(flowmapblue)

locations <- data.frame(
  id = c200ze$IRIS,
  lat = idINS2lonlat(c200ze$idINS)[1],
  lon = idINS2lonlat(c200ze$idINS)[2]
)

enqmobpro <- read_excel("~/files/base-flux-mobilite-domicile-lieu-travail-2019.xlsx", skip=5, sheet="Flux_sup_100")

flows <- data.frame(
  origin = enqmobpro$CODGEO,
  dest = enqmobpro$DCLT,
  count = enqmobpro$NBFLUX_C19_ACTOCC15P
)  

flowmapblue(locations, flows, mapboxAccessToken='pk.eyJ1IjoieHRpbWJlYXUiLCJhIjoiY2tmdzlqdmlrMDlrdzJybzhrZ3NkeXV1ZyJ9.dFDdu4vhAHXf9wIz38WGJw',
            clustering = TRUE, darkMode = TRUE, animation = FALSE)


#
com17_com <- com17 |> rename(CODGEO =insee )
enqmobpro <- merge(enqmobpro,com17_com,by='CODGEO')
com17_dclt <- com17 |> rename(DCLT =insee )
enqmobpro <- merge(enqmobpro,com17_dclt,by='DCLT')

enqmobpro$dist <- 0
for (i in 1:length(enqmobpro$CODGEO)) {
  enqmobpro$dist[i] <- as.numeric(st_distance(st_centroid(enqmobpro$geometry.x[i]), st_centroid(enqmobpro$geometry.y[i])))/1000
  }




#par commune d'origine 
dis_moyenne_commune <- function(data,commune) {
  sum_fd=0
  sum_f=0
  for (i in 1:length(data$CODGEO)) {
    if (commune==data$CODGEO[i]) {
      sum_fd <- sum_fd+data$NBFLUX_C19_ACTOCC15P[i]*data$dist[i]
      sum_f <- sum_f+data$NBFLUX_C19_ACTOCC15P[i]
    }
  }
  return(sum_fd/sum_f)
}

#pour Marseille
print(dis_moyenne_commune(enqmobpro,'13001')) #Aix-en-Provence
print(dis_moyenne_commune(enqmobpro,'13022'))
print(dis_moyenne_commune(enqmobpro,'13055'))


dist_moyenne_mars <- c()
for (k in 1:length(scot_tot.epci)) {
  dist_moyenne_mars <- append(dist_moyenne_mars,dis_moyenne_commune(enqmobpro,scot_tot.epci[k]))
}

dist_moyenne_mars <- data.frame(scot_tot.epci, dist_moyenne_mars)
dist_moyenne_mars <-  dist_moyenne_mars |> rename(insee = scot_tot.epci)
dist_moyenne_mars <- dist_moyenne_mars |> inner_join(com17, by='insee')
dist_moyenne_mars <-  dist_moyenne_mars |> rename(dist = dist_moyenne_mars)
dist_moyenne_mars <-  dist_moyenne_mars[21:118,]

dist_moyenne_mars <- st_as_sf(dist_moyenne_mars)

tmap_mode('view')
tm_shape(dist_moyenne_mars)+
  tm_polygons('dist')+  
  tm_borders(col = , lwd  = 0.5)

  
#par aire urbaine
AU <- read_excel("~/files/AU2010_au_01-01-2020.xlsx", skip=5, sheet="Composition_communale")

enqmobpro <- enqmobpro |> inner_join(AU, by='CODGEO')


dis_moyenne_AU <- function(data,AU) {
  sum_fd=0
  sum_f=0
  for (i in 1:length(data$CODGEO)) {
    if (AU==data$AU2010[i]) {
      sum_fd <- sum_fd+data$NBFLUX_C19_ACTOCC15P[i]*data$dist[i]
      sum_f <- sum_f+data$NBFLUX_C19_ACTOCC15P[i]
    }
  }
  return(sum_fd/sum_f)
}

#pour l'aire urbaine Lyon
dis_moyenne_AU(enqmobpro,"002")

#pour l'aire urbaine Marseille-Aix-Provence
dis_moyenne_AU(enqmobpro,"003")

#pour l'aire urbaine Belley
dis_moyenne_AU(enqmobpro,"294")

AU <- unique(enqmobpro$AU2010)
dist <- c()
for (k in 1:length(AU)) {
  dist <- append(dist,dis_moyenne_AU(enqmobpro,AU[k]))
}
dist_moyenne_AU <- data.frame(AU, dist)

dist_moyenne_AU <- dist_moyenne_AU[1:689,]
dist <- dist[1:689]

#stat desc de la variance
summary(dist)
var(dist)
sd(dist)