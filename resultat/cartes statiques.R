library(knitr)
library(sf)
library(tidyverse)
library(showtext)
library(mapdeck)
library(ofce)
library(downloadthis)
library(gt)
library(tmap)
library(glue)
library(ggspatial)

opts_chunk$set(
  echo = FALSE,
  warning = FALSE,
  message = FALSE,
  fig.pos="H", 
  out.extra="",
  dev="ragg_png",
  fig.showtext=TRUE,
  cache=FALSE)

source("secrets/azure.R")

showtext_opts(dpi=120)
showtext_auto()

trim <- function(x, xm, xp) ifelse( x<= xm, xm, ifelse(x>= xp, xp, x))

communes <- bd_read("communes")
centre <- communes |> 
  filter(INSEE_COM=="13215") |> 
  st_transform(4326) |> 
  st_centroid() |> 
  st_coordinates()
centre <- as.vector(centre)

c200ze <- bd_read("c200ze") |> st_transform(4326)

data <- c200ze |> 
  mutate(
    act = act_mobpro/4) |> 
  filter(ind > 0, scot) 
xlim <- c(st_bbox(c200ze)$xmin, st_bbox(c200ze)$xmax)
ylim <- c(st_bbox(c200ze)$ymin, st_bbox(c200ze)$ymax)

res <- ggplot() +
  bd_read("decor_carte_large") +
  geom_sf(
    data= data,
    mapping= aes(fill=act), col=NA) + 
  scale_fill_viridis_c(
    option="viridis",
    trans = 'log',
    direction=-1,
    name = "actifs par ha",
    breaks = c(1, 10, 100), 
    limits = c(1,200),
    oob = scales::squish) +
  theme_ofce_void() +
  coord_sf(xlim=xlim, ylim=ylim, crs=4326) + 
  theme(legend.position = "bottom", legend.key.height = unit(6, "pt")) +
  labs(caption=glue("*Source* : MOBPRO, C200"))

emp <- ggplot() +
  bd_read("decor_carte_large") +
  geom_sf(
    data= c200ze |> filter(emp_resident>0),
    mapping= aes(fill=emp_resident/4), col=NA) + 
  scale_fill_viridis_c(
    option="viridis",
    trans = 'log',
    direction=-1,
    name = "Emploi par ha",
    breaks = c(1, 10, 100),
    limits = c(1, 200),
    oob = scales::squish) +
  coord_sf(xlim=xlim, ylim=ylim, crs=4326) + 
  theme_ofce_void() +
  theme(legend.position = "bottom", legend.key.height = unit(6, "pt")) +
  labs(caption=glue("*Source* : MOBPRO, C200"))

library(patchwork)
carte1 <- res+emp
ofce::graph2png(carte1, ratio = 16/9)