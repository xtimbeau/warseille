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


#pour voir la distance entre les villes
com17_com <- as.data.frame(com17)
com17_dclt <- as.data.frame(com17)

enqmobpro$dist <- 0
for (i in 1:length(enqmobpro$CODGEO)) {
  for (j in 1:length(com17_com$insee)) {
    for (k in 1:length(com17_dclt$insee)) {
      if (enqmobpro$CODGEO[i]==com17_com$insee[j] & enqmobpro$DCLT[i]==com17_dclt$insee[k]) {
        enqmobpro$dist[i] <- as.numeric(st_distance(st_centroid(com17[j,]), st_centroid(com17[k,])))/1000
      }
    }
  }
}

#par commune d'origine 
dis_moyenne_commune <- function(data,commune) {
  sum_fd=0
  sum_f=0
  for (i in range(length(data[1]))) {
    if (commune==data['CODGEO'][i,]) {
      sum_fd <- sum_fd+data['NBFLUX_C19_ACTOCC15P'][i,]*data['dist'][i,]
      sum_f <- sum_f+data['NBFLUX_C19_ACTOCC15P'][i,]
    }
  }
  return(sum_fd/sum_f)
}

dis_moyenne(enqmobpro,'13001')

#par aire urbaine
dis_moyenne_AU <- function(data,AU) {
  sum_fd=0
  sum_f=0
  for (i in range(length(data[1]))) {
    if (AU==data['AU'][i,]) {
      sum_fd <- sum_fd+data['NBFLUX_C19_ACTOCC15P'][i,]*data['dist'][i,]
      sum_f <- sum_f+data['NBFLUX_C19_ACTOCC15P'][i,]
    }
  }
  return(sum_fd/sum_f)
}

dis_moyenne(enqmobpro,"Marseille-Aix-Provence")

