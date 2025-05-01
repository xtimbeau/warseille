library(marquee)
library(tidyverse)
library(ofce)

temps_tc <- set_names(c("7m w", "7m tc/w", "15m tc/w", "7m b", "15m b", "10m c", "15m c"), 
                      c("7' m.", "7' TC", "15' /TC", "7' vélo", "15' vélo", "10' voit.",  "15' voit."))
temps_clair <- set_names(
  c("7 minutes en marchant", "7 minutes en TC", "15 minutes en TC", 
    "7 minutes en vélo", "15 minutes en vélo", "10 minutes en voiture", "15 minutes en voiture"),
                         temps_tc)
vqh <- bd_read("vqh") |>
  mutate(wqse = factor(wqse))

lorenz <- vqh |> 
  group_by(type, temps) |> 
  arrange(se) |> 
  mutate(cumind = cumsum(ind)/sum(ind)) |> 
  ungroup()

lorenz_redux <- lorenz|> 
  mutate(
    cumind = ceiling(cumind*100)/100) |> 
  group_by(cumind, type, temps) |> 
  summarize(
    se = last(se),
    mse = mean(se),
    div = mean(div),
    sa = mean(sa),
    sb = mean(sb),
    n = mean(n),
    n_e = mean(n_e)) |> 
  group_by(type) |> 
  mutate(rse = pmax(log(se), 0)/max(log(se))) |> 
  ungroup() |> 
  mutate(
    type = factor(type, c("alim", "sante", "comm", "sortie")),
    tooltip = glue(
      "<b>{type}</b>
      {round(100*cumind)}% des résidents ont une proximité aux aménités d'au plus {round(mse)} ({round(rse*100)}% du max) pour un rayon de {temps}
      Surface équivalente moyenne : {round(mse)}
      Diversité moyenne : {round(div,1)}
      Surface ajustée moyenne : {round(sa,1)}
      Surface : {round(sb)}m²
      Nbre d'unités : {round(n)}
      Nbre d'espèces : {round(n_e, 1)}"))

src <- str_c("C200, OSM, fichiers fonciers")

lplots <- map(temps_tc, ~ {
  dl <- lorenz |> 
    filter(temps==.x) |> 
    select(cumind, se, type)
  dlr <- lorenz_redux |> 
    filter(temps == .x) |> 
    select(cumind, se, type, tooltip)
  ma <- dl |> 
    group_by(type) |> 
    filter(se >= 1) |>
    filter(se == min(se)) |> 
    mutate(cumind_s = str_c((100*cumind) |> round()," %")) |> 
    pull(cumind_s, name = type) 
  
  ggplot(dl) + 
    aes(x=cumind, y = se, color = type) +
    geom_step() +
    geom_point_interactive(
      shape = 1, size = 0.1, hover_nearest = TRUE,
      data = dlr,
      aes(tooltip = tooltip, data_id = cumind))+
    theme_ofce(
      marquee=TRUE,
      plot.title.position = "plot",
      legend.title = element_marquee(),
      legend.text = element_marquee(),
      legend.position = c(0.05, 0.95),
      legend.justification = c(0,1),
      legend.direction = "vertical" ) + 
    guides(color = guide_marquee(title = "Proximité aux !!1, !!2, !!3 ou !!4")) +
    xlab("% de la population de la métropole AMP") +
    ylab("Proximité moyenne des aménités") +
    scale_color_manual(values = c(alim = "purple4", sortie = "orange2", sante = "seagreen", comm = "yellow2"),
                       labels = c("Commerces alimentaires", "Santé humaine", "Commerces non alimentaires", "Sorties")) +
    scale_x_continuous(breaks = scales::breaks_width(0.1), labels = scales::label_percent(1)) +
    scale_y_log10(limits = c(NA, 5000)) +
    ofce_caption(
      source = src,
      note = "Les individus sont classés dans l'ordre de leur proximité aux aménités. {ma[['alim']]} de la population d'AMP n'a accès à aucun commerce alimentaire, contre {ma[['comm']]} pour les commerces non alimentaires, {ma[['sante']]} pour la santé humaine et {ma[['sortie']]} pour les bars, restaurants et cinéma pour un rayon de {temps_clair[[.x]]}.") }
)

return(lplots)