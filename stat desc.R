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

mobpro <- mobpro |> inner_join(com17, by='CODGEO')
mobpro <- mobpro |> mutate(dist=idINS2dist(mobpro$CODGEO, mobpro$DCLT))
