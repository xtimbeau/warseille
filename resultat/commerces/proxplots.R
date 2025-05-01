library(tidyverse)
library(ofce)
library(purrr)
library(stars)
library(sf)
library(paletteer)

legend_h <- list(
  theme_ofce_void(), 
  coord_sf(crs = 4326),
  theme(
    legend.position = "bottom",
    legend.box.spacing = unit(3, "pt"),
    legend.title = marquee::element_marquee(vjust=1.2, margin = margin(t=0, b=0, r=6)),
    legend.text = marquee::element_marquee(size = rel(0.6), margin = margin(t=1, b=1), width = NULL, hjust=0.5),
    legend.text.position = "bottom", 
    legend.key.spacing.x = unit(1, "pt"),
    legend.key.height = unit(6, "pt") ) )

dc <- bd_read("decor_carte")

vqh <- bd_read("vqh")

temps_tc <- set_names(
  c("7m w", "7m tc/w", "15m tc/w", "7m b", "15m b", "10m c", "15m c"),
  c("7m. m.", "7m. m./TC", "15m. m./TC", "7m. vélo", "15m. vélo", "10m. voit.", "15m. voit."))
temps_clair <- set_names(
  c("7 minutes en marchant", "7 minutes en TC", "15 minutes en TC", 
    "7 minutes en vélo", "15 minutes en vélo", "10 minutes en voiture", "15 minutes en voiture"),
  temps_tc)
commerces <- set_names(c("alim", "comm", "sante", "sortie"), c("Alimentaire", "Commerces", "Santé humaine", "Sorties"))
inv_commerces <- set_names(names(commerces), commerces)
plots <- map(commerces, \(.type) {
  map(temps_tc, \(.temps){
    data <- vqh |> 
      filter(type == .type) |> 
      filter(temps == .temps) |> 
      select(idINS = fromidINS, wqse_tt) |> 
      r3035::sidINS2sf() 
    ggplot(data)+
      dc +
      geom_sf(
        mapping= aes(fill = factor(wqse_tt)), col="white", linewidth = 0.01) + 
      scale_fill_paletteer_d("fishualize::Bodianus_rufus", direction=1, 
                             name = glue("Proximité {.type} : "), label = c("faible", "", "", "", "élevée"))+
      legend_h +
      guides(fill = guide_legend(nrow=1))+
      ofce::ofce_caption(
        source=glue("C200, OSM, Fichiers fonciers 2023"),
        note ="L'indicateur de proximité est calculé pour les aménités de type {inv_commerces[[.type]]} dans un rayon de {temps_clair[[.temps]]}. Les quintiles sont calculés pour la proximité en voiture, 10 minutes de rayon." |> glue())
  })
})

return(plots) 