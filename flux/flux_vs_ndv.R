library(tidyverse)
library(conflicted)
library(tmap)
library(ofce)
library(sf)
library(here)
library(archive)
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
coms_AMP <- com2021epci |> pull(INSEE_COM)
dv3f <- dv3f |>
  mutate(com = str_remove_all(l_codinsee, "\\{|\\}")) |> 
  # mutate(com = ifelse(str_detect(com, "^132"), "13055", com)) |> 
  filter(com %in% coms_AMP, filtre%in%c("0", "L")) |> 
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
  filter(!is.na(valeurfonc), !is.na(surface)) |> 
  summarize(vf = sum(valeurfonc), surf = sum(surface), n = n(), prix = vf/surf)

prix <- dv3f.c200 |>
  left_join(c200ze |> st_drop_geometry() |> select(idINS=fromidINS, IRIS), by = "idINS") |> 
  group_by(IRIS, anneemut) |> 
  summarize(prix = sum(vf)/sum(surf), n = sum(n)) |> 
  filter(anneemut%in%c(2022, 2021, 2020, 2019, 2011)) |> 
  pivot_wider(names_from = anneemut, values_from = c(n,prix)) |> 
  mutate(tx = (prix_2022/prix_2011)^(1/12)-1) |> 
  ungroup() |> 
  mutate(
    prix = ifelse(is.na(prix_2022), ifelse(is.na(prix_2021), ifelse(is.na(prix_2020), prix_2019, prix_2020), prix_2021), prix_2022)
  )

km_iris <- km |>
  group_by(IRIS) |> 
  summarize(km_pa = sum(km_i)/sum(f_i),
            f_i = sum(f_i),
            co2_i = sum(co2_i),
            n = n(),
            snv = sum(ind_snv)/sum(ind),
            ind = sum(ind)) |>
  left_join(prix, by=c("IRIS")) |> 
  select(IRIS, km_pa, f_i, co2_i, n, snv, ind, tx, prix) |> 
  mutate(shape = case_when(
    str_detect(IRIS, "^132") ~ "Marseille",
    str_detect(IRIS, "^13001") ~ "Aix-en-Provence",
    TRUE ~ "autre"),
    dens = 1/4/n*f_i)

bd_write(km_iris)
km_iris <- bd_read("km_iris")

(base <- ggplot(km_iris) +
    aes(x=snv, y=km_pa, fill = prix, weights=f_i) +
    scale_fill_distiller(palette="Spectral", 
                         trans="log", direction = -1,
                         oob = scales::squish,
                         limits = c(1000, 8000),
                         aesthetics = c( "fill"),
                         breaks = c(1000, 3000, 8000),
                         name="prix immobilier\n€/m² 2022")+
    geom_point(aes(size = dens, shape = shape), 
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
  geom_density(aes(x=snv, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  theme_ofce_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km_iris)+
  geom_density(aes(x=km_pa, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_ofce_void()+
  theme(plot.margin = margin())

distrev <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"),
        legend.key.width = unit(12, 'pt'),
        legend.key.spacing = unit(2, 'pt'))

bd_write(distrev)

# c200 -------------
# c'est pas ultra lisible

km <- km |> left_join(prix, by=c("IRIS"))
(base <- ggplot(km |> st_drop_geometry()) +
    aes(x=ind_snv/ind, y=km_pa, fill = prix_2022, weights=f_i) +
    scale_fill_distiller(palette="Spectral", 
                         trans="log", direction = -1,
                         oob = scales::squish,
                         limits = c(1000, 8000),
                         aesthetics = c( "fill"),
                         breaks = c(1000, 3000, 8000),
                         name="prix immobilier\n€/m² 2022")+
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
  theme_ofce_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km)+
  geom_density(aes(x=km_pa, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_ofce_void()+
  theme(plot.margin = margin())

distrev_c200 <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"),
        legend.key.width = unit(12, 'pt'),
        legend.key.spacing = unit(2, 'pt'))

bd_write(distrev_c200)

# graphiques alternatifs -----------------

## alt1 : co2 versus densité ------------

(base <- ggplot(km_iris |> st_drop_geometry()) +
   aes(x=dens, y=co2_i/f_i, fill = prix, weights=f_i) +
   scale_fill_distiller(palette="Spectral", 
                        trans="log", direction = -1,
                        oob = scales::squish,
                        limits = c(1000, 8000),
                        aesthetics = c( "fill"),
                        breaks = c(1000, 3000, 8000),
                        name="prix immobilier (IRIS)\n€/m² 2022")+
   geom_point(aes(size = dens), 
              alpha=0.95, shape = 21, color="transparent", stroke=0.1) +
   guides(size=guide_legend(title = "Actifs/ha", override.aes = list(color="grey25"))) + 
   scale_x_continuous("Densité (actifs/ha)", labels = scales::label_number(big.mark = " ")) + 
   scale_y_continuous("tonnes de CO~2~ par an par actif motif professionnel", labels = scales::label_number(big.mark = " ")) +
   geom_smooth(col="gold3", fill = "gold", aes(weight = f_i), alpha=0.1) +
   theme_ofce(base_size = 10, legend.position = "bottom" )+
   theme(plot.margin = margin(),
         axis.title.y = ggtext::element_markdown()))

top_dens <- ggplot(km_iris)+
  geom_density(aes(x=dens, y=after_stat(density), weight=f_i), 
               color = "black", fill="gold", alpha=0.25, linewidth=0.2)+
  theme_ofce_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km_iris)+
  geom_density(aes(x=co2_i/f_i, y=after_stat(density), weight=f_i), 
               color = "black", fill="gold", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_ofce_void()+
  theme(plot.margin = margin())

co2dens <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"),
        legend.key.width = unit(12, 'pt'),
        legend.key.spacing = unit(2, 'pt'))

bd_write(co2dens)

## dynamique de construction ou de densité ----

aa <- archive_extract("https://www.insee.fr/fr/statistiques/fichier/7704076/base-ic-evol-struct-pop-2020_csv.zip",
                      dir = "/tmp/")
iris20 <- vroom::vroom(str_c("/tmp/", aa[[1]])) |> 
  select(pop20 = P20_POP, IRIS)

aa <- archive_extract("https://www.insee.fr/fr/statistiques/fichier/2386737/base-ic-evol-struct-pop-2013.zip",
                      dir = "/tmp/")
iris13 <- readxl::read_xls(str_c("/tmp/", aa[[1]]), skip = 5) |> 
  select(pop13 = P13_POP, IRIS)

km_iris_dpop <- km_iris |> 
  left_join(iris20, by="IRIS") |> 
  left_join(iris13, by="IRIS") |> 
  mutate(dpop = (pop20/pop13)^(1/(20-13))-1)

(base <- ggplot(km_iris_dpop |> filter(pop20>1000)) +
    aes(x=dpop, y=co2_i/f_i, fill = prix, weights=f_i) +
    scale_fill_distiller(palette="Spectral", 
                         trans="log", direction = -1,
                         aesthetics = c( "fill"),
                         oob = scales::squish,
                         limits = c(1000, 8000),
                         breaks = c(1000, 3000, 8000),
                         name="prix immobilier\n€/m² 2022")+
    geom_smooth(col="gold", fill = "gold", aes(weight = f_i), alpha=0.1) +
    geom_point(aes(size = dens, shape = shape), 
               alpha=0.95, stroke=.1, color = "transparent") +
    geom_vline(xintercept = 0, linetype="dotted", color = "grey") +
    scale_shape_manual(values=c("Marseille"=22, "Aix-en-Provence"=23, "autre"=21)) +
    guides(size=guide_legend(title = "Actifs/ha", override.aes = list(color="grey25")),
           shape = "none") + 
    scale_x_continuous("Evolution annuelle 2013-2020 de la population",
                       labels = scales::label_percent(.1), limits = c(-0.05, 0.05)) + 
    scale_y_continuous("km parcourus pour le motif professionel (moyenne de l'IRIS)", labels = scales::label_number(big.mark = " ")) +
    theme_ofce(base_size = 10, legend.position = "bottom") )+
  theme(plot.margin = margin())

top_dens <- ggplot(km_iris)+
  geom_density(aes(x=snv, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  theme_ofce_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km_iris)+
  geom_density(aes(x=km_pa, y=after_stat(density), weight=f_i), 
               color = "black", fill="pink", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_ofce_void()+
  theme(plot.margin = margin())

dpopco2 <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"),
        legend.key.width = unit(12, 'pt'),
        legend.key.spacing = unit(2, 'pt'))

bd_write(dpopco2)
