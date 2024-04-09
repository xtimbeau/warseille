library(knitr)
library(sf)
library(tidyverse)
library(showtext)
library(mapdeck)
library(ofce)
library(downloadthis)
library(gt)
library(tmap)

opts_chunk$set(
  echo = FALSE,
  warning = FALSE,
  message = FALSE,
  fig.pos="H", 
  out.extra="",
  dev="ragg_png",
  fig.showtext=TRUE,
  cache=FALSE)
source("../secrets/azure.R")
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

tkn <- Sys.getenv("mapbox_token")
mapdeck::set_token(tkn)

style <- "mapbox://styles/xtimbeau/ckyx5exex000r15n0rljbh8od"
  
  
#  style <-"mapbox://styles/mapbox/light-v11"