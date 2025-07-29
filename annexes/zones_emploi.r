load("baselayer.rda")

library(data.table)
library(sf)
library(tidyverse)

c200 <- qs::qread(c200ze_file)
ze.communes <- c200 |> pull(com22) |> unique() # la géographie de mobpro 2018 est celle de 2021

# ---- Fichier Mobilité professionnelle 2018 ----
mobpro <- fread(file = "~/files/FD_MOBPRO_2019.csv")

com22_scot <- filter(c200, scot) |> pull(com22) |> unique() # communes du scot selmon la géométrie 2021

mobpro[, live_in := (COMMUNE %in% com22_scot)]
mobpro[, work_in := (DCLT %in% ze.communes)]

mobpro[, statut := ifelse(STAT == 10,
                          ifelse(NA5 == "OQ", "sal_public", "sal_prive"), 
                          ifelse(between(STAT, 21, 23), "non_sal", NA_character_))]

mobilites <- mobpro[, .(NB = sum(IPONDI), live_in = first(live_in), work_in = first(work_in)), 
                    by = c("COMMUNE", "DCLT", "statut", "TRANS")]

# filtre sur la zone d'étude
mobilites <- mobilites[ live_in == TRUE | work_in == TRUE,]

mobilites[, TRANS := factor(TRANS) |> 
            fct_recode("none" = "1","walk" = "2", "bike" = "3", "car" = "4", "car" = "5", "transit" = "6")]

# les immobiles = aucun mode de transport
les_immobiles <- mobilites[TRANS == "none" & live_in == TRUE, ][, .(NB = sum(NB)), by = c("COMMUNE", "statut")] |> 
  as_tibble() |> 
  pivot_wider(names_from = statut, values_from = NB, values_fill = 0)

# les mobiles selon qu'ils viennent ou non de la zone centrale et par statut.
# le statut sert à renormaliser car on ne situe que l'emploi privé pour l'instant.
les_mobiles <- mobilites[TRANS != "none",][, .(NB = sum(NB), live_in = first(live_in), work_in = first(work_in)), 
                                           by = c("COMMUNE", "DCLT", "statut")]

# les fuites = les résidents qui ne travaillent pas dans la zone
les_fuites <- les_mobiles[live_in == TRUE, ][, .(NB = sum(NB), work_in = first(work_in)), 
                                             by = c("COMMUNE", "DCLT") ] |> 
  as_tibble() |> 
  mutate(work_in = ifelse(work_in, "in_ze", "out_ze")) |> 
  pivot_wider(names_from = work_in, values_from = NB, values_fill = 0)

les_fuites <- les_fuites |> 
  group_by(COMMUNE) |> 
  summarise(in_ze = sum(in_ze), out_ze = sum(out_ze)) |> 
  mutate(fuite = out_ze / (in_ze + out_ze))

les_mobiles <- les_mobiles[work_in == TRUE,] |> 
  as_tibble() |> 
  mutate(live_in = ifelse(live_in, "In", "Out")) |> 
  group_by(DCLT, statut, live_in) |> 
  summarise(NB = sum(NB)) |> 
  pivot_wider(names_from = live_in, values_from = NB, values_fill = 0) |> 
  dplyr::filter(In > 0) |> 
  mutate(taux_in = In / (In +Out)) |> 
  ungroup()

save(les_mobiles, les_immobiles, les_fuites, mobilites, file = "marges_mobpro.rda")
