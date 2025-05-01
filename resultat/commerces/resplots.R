library(tidyverse)
library(ofce)
library(purrr)
library(stars)
library(sf)
library(paletteer)

lien_se_si <- bd_read("lien_se_si")

thm_map_leg <- list(
  scale_fill_paletteer_c(palette = "ggthemes::Red-Green Diverging", 
                         aesthetics = c("fill", "color"),
                         labels = c("En déficit","","","", "En excès"),
                         breaks = c(-5,-2.5,0, 2.5, 5),
                         trans = pseudo_log_trans(), 
                         name = "Aménités par rapport à la densité de population :"),
  legend_h, 
  theme(legend.key.width = unit(1, "cm"),
        legend.text = element_blank()),
  theme(legend.text = element_text(size = rel(0.4))))

cat <- c("se_alim", "se_comm", "se_sante", "se_sortie", "se_agg")
labels <- c("commerces alimentaires", "commerces (non alimentaires)",
            "établissement de santé humaine", "établissements de sorties", "commerces synthétiques (combinaison pondérée)")
labels <- set_names(labels, cat)

plots <- map(cat, ~{ 
  data <- lien_se_si |>
    filter(t == .x, m == 1) |>
    pull(residus) |>
    pluck(1) |>
    r3035::sidINS2sf() |> drop_na(r) 
  
  ggplot() +
    bd_read("decor_carte") +
    geom_sf( data = data, aes(fill = r, col = r)) +
    thm_map_leg +
    ofce_caption(
      source = "C200, Fichiers Fonciers, OSM, calculs des auteurs",
      note = "La carte représente le résidu d'une équation reliant proximité (variable expliquée) aux {labels[[.x]]} et densité (variable explicative) en log-log. La couleur rouge (verte) représente ainsi un déficit (excès) de proximité aux aménités par rapport à la densité de résidents.")
})

return(plots)