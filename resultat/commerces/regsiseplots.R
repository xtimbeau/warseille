lien_se_si <- bd_read("lien_se_si")

legend_h <- list(
  theme_ofce_void(), 
  theme(
    legend.position = "bottom",
    legend.box.spacing = unit(3, "pt"),
    legend.title = marquee::element_marquee(vjust=1.2, margin = margin(t=0, b=0, r=6)),
    legend.text = marquee::element_marquee(size = rel(0.6), margin = margin(t=1, b=1), width = NULL, hjust=0.5),
    legend.text.position = "bottom", 
    legend.key.spacing.x = unit(1, "pt"),
    legend.key.height = unit(6, "pt") ) )

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

commerces <- set_names(c("se_alim", "se_comm", "se_sante", "se_sortie", "se_agg"), c("Alimentaire", "Commerces", "Santé humaine", "Sorties", "Synthétique"))
inv_commerces <- set_names(names(commerces), commerces)

plots <- map(commerces, ~{
  
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
      note = "La carte représente le résidu d'une équation reliant proximité (variable expliquée) et densité (variable explicative) en log-log. 
      La couleur rouge représente ainsi un déficit de proximité aux aménités (ici {inv_commerces[[.x]]}) par rapport à la densité.")
})


return(plots)