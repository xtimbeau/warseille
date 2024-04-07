library(tidyverse)
library(conflicted)
library(tmap)
library(ofce)
library(sf)
library(here)
source(here("secrets/azure.R"))

conflict_prefer_all("dplyr", quiet=TRUE)
load("baselayer.rda")

c200ze <- bd_read("c200ze") |> 
  select(fromidINS=idINS, ind_snv, ind, IRIS) |>
  st_drop_geometry()

km <- bd_read("meaps_from") |>
  as_tibble() |> 
  left_join(c200ze, by="fromidINS") |> 
  mutate(ndv  = ind_snv/ind)

km_iris <- km |>
  group_by(IRIS) |> 
  summarize(km_pa = sum(km_i)/sum(f_i),
            f_i = sum(f_i),
            n = n(),
            ind_snv = sum(ind_snv)/sum(ind),
            ind = sum(ind))

ggplot(km) +
  aes(x=ind_snv/ind, y=km_pa, color = com) +
  geom_point(alpha=0.1, shape = 19) +
  scale_x_log10("Niveau de vie moyen du carreau", labels = scales::label_number(big.mark = " "), limits = c(15000, 35000)) + 
  scale_y_log10("km parcourus pour le motif professionel", labels = scales::label_number(big.mark = " ")) +
  geom_smooth(col="darkgrey") +
  theme_ofce() +
  labs(title = "Km versus niveau de vie, La Rochelle")+
  guides(color = "none") 


# Les prix !

pak::pak('nuvolos-cloud/r-nuvolos-tools')
library(tidyverse)
library(r3035)
library(sf)
library(nuvolos)

con <- get_connection()
db <- tbl(con, "DV3FV231_MUTATION") 

cols <- c("idnatmut", "datemut", "anneemut",  "moismut", "l_codinsee",   
          "coddep", "libnatmut", "vefa", "valeurfonc", "sterr", "sbati", "codtypbien", "libtypbien", "filtre",
          "devenir", "lon", "lat")
COLS <- toupper(cols)

dv3f <- db |> dplyr::select(all_of(COLS)) |> rename_with(tolower)
coms_AMP <- scot_tot.epci
dv3f <- dv3f |>
  mutate(com = str_remove_all(l_codinsee, "\\{|\\}")) |> 
  mutate(com = ifelse(str_detect(com, "^132"), "13055", com)) |> 
  filter(com %in% scot_tot.epci, filtre=="0") |> 
  collect() |> 
  mutate(
    typebien = case_when(
      str_detect(codtypbien, "^111") ~ "maison",
      str_detect(codtypbien, "^121") ~ "appartement",
      str_detect(codtypbien, "^11") ~ "maisons",
      str_detect(codtypbien, "^12") ~ "appartements",
      str_detect(codtypbien, "^13") ~ "dépendances",
      str_detect(codtypbien, "^14") ~ "activité",
      str_detect(codtypbien, "^15") ~ "mixte",
      str_detect(codtypbien, "^2") ~ "terrain", 
      TRUE ~"autres"),
    surface = sbati,
    prixm2 = valeurfonc/surface,
    idINS = r3035::sidINS3035(sf_project(from=st_crs(4326), to=st_crs(3035), cbind(.data$lon, .data$lat))))

dv3f.c200 <- dv3f |> 
  group_by(idINS, anneemut) |> 
  filter(typebien %in% c("maison", "appartement")) |> 
  summarize(vf = sum(valeurfonc), surf = sum(surface), n = n(), prix = vf/surf)

prix <- dv3f.c200 |>
  left_join(c200ze |> st_drop_geometry() |> select(idINS=fromidINS, IRIS)) |> 
  group_by(IRIS, anneemut) |> 
  summarize(prix = sum(vf)/sum(surf), n = sum(n)) |> 
  filter(anneemut%in%c(2022, 2021, 2011)) |> 
  pivot_wider(names_from = anneemut, values_from = c(n,prix)) |> 
  mutate(tx = (prix_2022/prix_2011)^(1/12)-1) |> 
  ungroup()

km_iris <- km_iris |> left_join(prix, by=c("IRIS"))  

bd_write(km_iris)
km_iris <- bd_read("km_iris") |> 
  mutate(shape = case_when(
    str_detect(IRIS, "^132") ~ "Marseille",
    str_detect(IRIS, "^13001") ~ "Aix-en-Provence",
    TRUE ~ "autre"))
(base <- ggplot(km_iris) +
    aes(x=ind_snv, y=km_pa, fill = prix_2022, weights=f_i) +
    scale_fill_distiller(palette="Spectral", 
                         trans="log", direction = -1,
                         aesthetics = c( "fill"),
                         breaks = c(1000, 3000, 8000),
                         name="prix immobilier (IRIS)\n€/m² 2022")+
    geom_point(aes(size = 1/4/n*f_i, shape = shape), 
               alpha=0.95, stroke=.1, color = "transparent") +
    scale_shape_manual(values=c("Marseille"=22, "Aix-en-Provence"=23, "autre"=21)) +
    guides(size=guide_legend(title = "Actifs/ha", override.aes = list(color="grey25")),
           shape = "none") + 
    scale_x_continuous("Niveau de vie moyen de l'IRIS", labels = scales::label_number(big.mark = " "), limits = c(15000, 35000)) + 
    scale_y_continuous("km parcourus pour le motif professionel (moyenne de l'IRIS)", labels = scales::label_number(big.mark = " ")) +
    geom_smooth(col="lightblue", fill = "lightblue1", aes(weight = f_i)) +
    theme_ofce(base_size = 10, legend.position = "bottom") )+
  theme(plot.margin = margin())

top_dens <- ggplot(km_iris)+
  geom_density(aes(x=ind_snv, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  theme_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km_iris)+
  geom_density(aes(x=km_pa, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_void()+
  theme(plot.margin = margin())

distrev <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"))

bd_write(distrev)

# c200 -------------
# c'est pas ultra lisible

km <- km |> left_join(prix, by=c("IRIS"))
(base <- ggplot(km |> st_drop_geometry()) +
    aes(x=ind_snv/ind, y=km_pa, fill = prix_2022, weights=f_i) +
    scale_fill_distiller(palette="Spectral", 
                         trans="log", direction = -1,
                         aesthetics = c( "fill"),
                         breaks = c(1000, 3000, 8000),
                         name="prix immobilier (IRIS)\n€/m² 2022")+
    geom_point(aes(size = 1/4*f_i), 
               alpha=0.95, shape = 21, color="transparent", stroke=0.1) +
    guides(size=guide_legend(title = "Actifs/ha", override.aes = list(color="grey25"))) + 
    scale_x_continuous("Niveau de vie moyen du carreau", labels = scales::label_number(big.mark = " "), limits = c(15000, 35000)) + 
    scale_y_continuous("km parcourus pour le motif professionel", labels = scales::label_number(big.mark = " ")) +
    geom_smooth(col="lightblue", fill = "lightblue1", aes(weight = f_i)) +
    theme_ofce(base_size = 10, legend.position = "bottom") )+
  theme(plot.margin = margin())

top_dens <- ggplot(km)+
  geom_density(aes(x=ind_snv, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  theme_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km)+
  geom_density(aes(x=km_pa, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_void()+
  theme(plot.margin = margin())

distrev_c200 <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"))

bd_write(distrev_c200)
