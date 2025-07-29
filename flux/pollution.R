library(tidyverse)
library(arrow)
library(archive)
library(tmap)
library(sf)
library(stars)
library(MetricsWeighted)
library(ofce)
library(glue)
source("secrets/azure.R")
source("mglobals.r")
conflicted::conflicts_prefer(dplyr::filter)
curl::curl_download("https://geoservices.atmosud.org/geoserver/mod_sudpaca_2022/ows?service=WMS&version=1.1.1&request=GetMap&layers=mod_sudpaca_2022:mod_sudpaca_2022_icair365&styles=&bbox=799602.0,6214473.0,1077706.0,6453458.0&width=768&height=659&srs=EPSG:2154&format=image/geotiff",
              destfile = "/tmp/sudair.tiff")
version <- bd_read("version")
pol <- raster::raster("/tmp/sudair.tiff") |> 
  st_as_stars() 

c200ze <- qs::qread(c200ze_file) |> filter(scot)

r200 <- st_as_stars(st_bbox(c200ze), dx=200, dy=200)
bb <- st_bbox(c200ze)

pol3035.stars <- stars::st_warp(pol, r200) |> 
  mutate(sudair = ifelse(sudair==253, NA, sudair))


pol3035 <- as_tibble(pol3035.stars) |> mutate(idINS = r3035::sidINS3035(x,y))
dd <- 10000
inset_map <- ggplot()+geom_stars(data=pol3035.stars |> st_crop(r200), show.legend=FALSE)+ 
  coord_sf(xlim = c(bb$xmin+dd, bb$xmax-dd), ylim =c(bb$ymin+dd, bb$ymax-dd))+
  labs(caption = "ICAIR365 2022, AtmoSud") +
  theme_void()+
  theme(plot.caption = element_text(family="Open Sans", size = 6))+
  scale_fill_viridis_c(option="turbo", direction=1, na.value="transparent")

bd_write(inset_map)

km_iris <- bd_read("km_iris") |> 
  left_join(
    pol3035 |>
  left_join(c200ze |> st_drop_geometry() |> select(idINS, IRIS, ind), by="idINS" ) |>
  filter(!is.na(IRIS)) |> 
  group_by(IRIS) |> 
  summarize(
    sudair = weighted_mean(sudair, w = ind+0.001)) , by="IRIS") |> 
  ungroup()

bd_write(km_iris, name = "km_iris_pol")

(base <- ggplot(km_iris)+
  aes(y=km_pa, x=sudair, size=dens, fill = prix, shape = shape)+
  scale_fill_distiller(palette="Spectral", 
                       trans="log", direction = -1,
                       oob = scales::squish,
                       limits = c(1000, 8000),
                       aesthetics = c( "fill"),
                       breaks = c(1000, 3000, 8000),
                       name="prix immobilier\n€/m² 2022")+
  geom_point(alpha=0.95, stroke=.1, color = "transparent") + 
  scale_shape_manual(values=c("Marseille"=22, "Aix-en-Provence"=23, "autre"=21)) +
  guides(size=guide_legend(title = "Actifs/ha", 
                           override.aes = list(color="grey25")),
         shape = "none") + 
  scale_x_continuous("Indice Cumulé de pollution de l'AIR (ICAIR) annuel",
                     labels = scales::label_number(big.mark = " ")) + 
  scale_y_continuous("CO2 émis pour le motif professionel (moyenne par an de l'IRIS)", 
                     labels = scales::label_number(big.mark = " ")) +
  # geom_smooth(col="lightblue", fill = "lightblue1", aes(weight = f_i)) +
  theme_ofce(base_size = 10, legend.position = "bottom")+ 
  patchwork::inset_element(inset_map, left=0.7, bottom=0.63, right=1, top=1) + 
  theme(plot.margin = margin()))

top_dens <- ggplot(km_iris)+
  geom_density(aes(x=sudair, y=after_stat(density), weight=co2_i), 
               color = "black", fill="palegreen", alpha=0.25, linewidth=0.2)+
  theme_ofce_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km_iris)+
  geom_density(aes(x=km_pa, y=after_stat(density), weight=co2_i), 
               color = "black", fill="palegreen", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_ofce_void()+
  theme(plot.margin = margin())

poldist <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"),
        legend.key.width = unit(12, 'pt'),
        legend.key.spacing = unit(2, 'pt'))

bd_write(poldist)  

# graphique densité pollution (??)

(base <- ggplot(km_iris)+
    aes(y=dens, x=sudair, size=dens, fill = prix, shape = shape)+
    scale_fill_distiller(palette="Spectral", 
                         trans="log", direction = -1,
                         oob = scales::squish,
                         limits = c(1000, 8000),
                         aesthetics = c( "fill"),
                         breaks = c(1000, 3000, 8000),
                         name="prix immobilier\n€/m² 2022")+
    geom_point(alpha=0.95, stroke=.1, color = "transparent") + 
    scale_shape_manual(values=c("Marseille"=22, "Aix-en-Provence"=23, "autre"=21)) +
    guides(size=guide_legend(title = "Actifs/ha", 
                             override.aes = list(color="grey25")),
           shape = "none") + 
    scale_x_continuous("Indice Cumulé de pollution de l'AIR (ICAIR) annuel",
                       labels = scales::label_number(big.mark = " ")) + 
    scale_y_continuous("Densité de l'IRIS", 
                       labels = scales::label_number(big.mark = " ")) +
    # geom_smooth(col="lightblue", fill = "lightblue1", aes(weight = f_i)) +
    theme_ofce(base_size = 10, legend.position = "bottom")+ 
    labs(
      caption=glue::glue(
        "*Source* : MOBPRO, EMP 2019, C200, OSM, GTFS, DV3F CEREMA, MEAPS. *version {version}*
       <br>*Note* : Chacun des points représente un IRIS. Les carrés sont pour la commune de Marseille,
      <br> les losanges pour la commune d'Aix-en-Provence,
       <br>les ronds pour les autres communes.<br>")  ) +
    patchwork::inset_element(inset_map, left=0.7, bottom=0.63, right=1, top=1) + 
    theme(plot.margin = margin()))

top_dens <- ggplot(km_iris)+
  geom_density(aes(x=sudair, y=after_stat(density), weight=co2_i), 
               color = "black", fill="palegreen", alpha=0.25, linewidth=0.2)+
  theme_ofce_void()+
  theme(plot.margin = margin())
right_dens <- ggplot(km_iris)+
  geom_density(aes(x=dens, y=after_stat(density), weight=co2_i), 
               color = "black", fill="palegreen", alpha=0.25, linewidth=0.2)+
  coord_flip()+
  theme_ofce_void()+
  theme(plot.margin = margin())

poldens <- patchwork::wrap_plots(
  top_dens, patchwork::plot_spacer(), base,  right_dens,
  ncol=2, nrow=2, widths = c(1, 0.1), heights = c(0.1, 1)) &
  theme(panel.spacing = unit(0, "pt"), 
        legend.key.height = unit(6, "pt"),
        legend.key.width = unit(12, 'pt'),
        legend.key.spacing = unit(2, 'pt'))

bd_write(poldens)
