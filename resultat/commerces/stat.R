library(MetricsWeighted)
library(ofce)
library(tidyverse)

stat1  <- bd_read("commerces") |> 
  group_by(type, cconac) |>
  summarize(
    sw= first(w), 
    n = n(),
    s = sum(sprincp)) |> 
  group_by(type) |> 
  summarize(
    n_w = sum(sw)
  )

stat2 <-  bd_read("commerces") |> 
  group_by(type) |> 
  summarize(
    n_cconac = n_distinct(cconac),
    n = n(),
    s_mean = mean(sprincp),
    s_q10 = quantile(sprincp, probs = 0.1),
    s_q50 = quantile(sprincp, probs = 0.5),
    s_q90 = quantile(sprincp, probs = 0.9)
  )

stat3 <- bd_read("vqh") |> 
  group_by(type) |> 
  summarize(
    div_q10 = weighted_quantile(div, w = ind, probs = 0.1),
    div_q50 = weighted_quantile(div, w = ind, probs = 0.5),
    div_q90 = weighted_quantile(div, w = ind, probs = 0.9),
    sa_q10 = weighted_quantile(sa, w = ind, probs = 0.1),
    sa_q50 = weighted_quantile(sa, w = ind, probs = 0.5),
    sa_q90 = weighted_quantile(sa, w = ind, probs = 0.9),
    se_q10 = weighted_quantile(div*sa, w = ind, probs = 0.1),
    se_q50 = weighted_quantile(div*sa, w = ind, probs = 0.5),
    se_q90 = weighted_quantile(div*sa, w = ind, probs = 0.9)
  ) |> mutate(div_id = div_q90/div_q10,
              sa_id = sa_q90/sa_q10,
              se_id = se_q90/se_q10)

stat <- stat1 |> 
  left_join(stat2, by = "type") |> 
  left_join(stat3, by = "type") |> 
  mutate(type  = factor(
    type,
    c("alim", "comm", "sortie", "sante"),
    c("Alimentaire", "Commerces non alimentaires", "Bars, restaurants, cinémas", "Santé humaine")))

return(stat)