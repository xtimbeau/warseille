setwd("~/marseille")
# init ---------------
library(tidyverse)
library(tictoc)
library(accesstars) 
library(accessibility)
library(r3035)
library(glue)
library(stars)
library(data.table)
library(conflicted)
library(arrow)
library(furrr)

progressr::handlers(global = TRUE)
progressr::handlers("cli")
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer_all( "dplyr", quiet=TRUE)
conflict_prefer('wday', 'lubridate', quiet=TRUE)

## globals --------------------
load("baselayer.rda")

# ds <- open_dataset("DVFdata", partitioning = c("Communes"))

resol <- 200

c200ze <- qs::qread("{c200ze_file}" |> glue()) |> 
  filter(pze)

origines <- c200ze |>
  filter(ind>0, scot) |> 
  select(ind) |>
  st_centroid() 
opportunites <- c200ze |>
  filter(emp>0, mobpro99) |> 
  select(emplois=emp) |> 
  st_centroid() |> 
  st_transform(crs=4326)

oo <- origines |> 
  st_transform(4326) |> 
  st_coordinates()
colnames(oo) <- c("lon", "lat")

dd <- opportunites |> 
  st_transform(4326) |> 
  st_coordinates()
colnames(dd) <- c("lon", "lat")

dodgr_bike <- routing_setup_dodgr(glue("{mdir}/dodgr/"), 
                                  mode = "BICYCLE", 
                                  turn_penalty = TRUE,
                                  distances = TRUE,
                                  denivele = TRUE,
                                  n_threads = 8L,
                                  overwrite = TRUE,
                                  nofuture=FALSE)

gg <- accessibility:::load_streetnet(dodgr_bike$graph_name)
meso <- (gg$verts_c[match_points_to_verts(gg$verts_c, oo[1:100,]),])$id
mesd <- (gg$verts_c[match_points_to_verts(gg$verts_c, dd[1:100,]),])$id
graph <- dodgr:::preprocess_spatial_cols (dodgr:::tbl_to_df(gg$graph))
graph <- dodgr:::tbl_to_df (graph)
hps <- dodgr:::get_heap ("BHeap", graph)
heap <- hps$heap
graph <- hps$graph
graph <- dodgr:::preprocess_spatial_cols (graph)

pg <- process_graph(gg$graph)
  
tic();mm <- dodgr::dodgr_dists(gg$graph, meso, mesd);toc()
tic();mm2 <- dodgr::dodgr_dists(gg$graph, oo[1:100,], dd[1:100,]);toc()
tic();mm3 <- dodgr::dodgr_dists_pre(gg, oo[1:100,], dd[1:100,]);toc()
tic();mm4 <- dodgr::dodgr_dists_pre(pg, meso, mesd);toc()

to_from_indices <- dodgr:::to_from_index_with_tp (graph, meso, mesd)
