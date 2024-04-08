# check rt
rt.dt <- as.data.table(rt)
names(rt.dt) <- tos
rt.dt[, rn := froms]
rt.dt <- melt(rt.dt, id.vars="rn")
setnames(rt.dt, c("rn","variable","value"), c("fromidINS", "toidINS", "d_ij"))
rt.dt <- rt.dt[d_ij>0, ]
rt.dt[, `:=`(fromidINS = as.integer(as.character(fromidINS)),
             toidINS = as.integer(as.character(toidINS)))]

write_parquet(rt.dt, "/tmp/rt.parquet")
rm(rt.dt)
rt.dt <- open_dataset("/tmp/rt.parquet") |> to_duckdb()
rm(rt, rm)
gc()
distances.car <- open_dataset(dist_dts) |> 
  to_duckdb() |> 
  filter(mode %in% c("car_dgr")) |>
  select(fromidINS, toidINS, travel_time, distance)

dd <- distances.car |> 
  left_join(rt.dt, by=c("fromidINS", "toidINS")) |>
  filter(!is.na(d_ij))
dd

# meuc
meuc <- matrix(0, nrow = length(froms), ncol = length(tos), dimnames = dimnames(rm))
to_x <- round(as.numeric(tos) / 100000)
to_y <- as.numeric(tos) - to_x*100000
for(i in froms) {
  from_x <- round(as.numeric(i) / 100000)
  from_y <- as.numeric(i) - from_x*100000
  meuc[i,tos] <- 0.2 * sqrt((from_x-to_x)^2 + (from_y-to_y)^2)
}
reuc <- matrixStats::rowRanks(meuc, ties.method = "random")    
rm(meuc, rm, rt)
gc()


fake <- list()
fake$emplois <- masses_AMP$emplois/masses_AMP$emplois * mean(masses_AMP$emplois)
fake$actifs <- masses_AMP$actifs/masses_AMP$actifs * mean(masses_AMP$actifs)
fake$fuites <- masses_AMP$fuites/masses_AMP$fuites * mean(masses_AMP$emplois)/mean(masses_AMP$actifs)
fake$shufs <- emiette(les_actifs = fake$actifs, nshuf = 2, seuil = 500)

test1 <- meaps |> 
  full_join(distances.car, by = c("fromidINS","toidINS")) |> 
  group_by(toidINS) |> 
  summarize(d_j = sum(f_ij * distance, na.rm=TRUE), f_j = sum(f_ij, na.rm=TRUE)) |> 
  mutate(d_j = d_j/f_j) |> 
  collect() |> 
  left_join(c200ze |> select(toidINS = idINS, com), by="toidINS") |> 
  st_as_sf()

test2 <- meaps |> 
  left_join(distances.car, by = c("fromidINS","toidINS")) |> 
  group_by(fromidINS) |> 
  summarize(d_j = sum(f_ij * distance, na.rm=TRUE), f_j = sum(f_ij, na.rm=TRUE)) |> 
  mutate(d_j = d_j/f_j) |> 
  collect() |> 
  left_join(c200ze |> select(fromidINS = idINS, com), by="fromidINS") |> 
  st_as_sf()

test3 <- meaps |> 
  mutate(from_x = floor(fromidINS/100000), to_x = floor(toidINS/100000)) |> 
  mutate(from_y = fromidINS-from_x*100000, to_y = toidINS-to_x*100000) |>
  mutate(euc=.2*sqrt((from_x-to_x)^2+(from_y-to_y)^2)) |>
  group_by(toidINS) |>
  summarize(f_j = sum(f_ij), d_j = sum(f_ij*euc)) |>
  mutate(d_j = d_j/f_j) |> 
  collect() |> 
  left_join(c200ze |> select(toidINS = idINS), by="toidINS") |> 
  st_as_sf()

test4 <- distances.car |> 
  mutate(from_x = floor(fromidINS/100000), to_x = floor(toidINS/100000)) |> 
  mutate(from_y = fromidINS-from_x*100000, to_y = toidINS-to_x*100000) |>
  mutate(euc=.2*sqrt((from_x-to_x)^2+(from_y-to_y)^2)) |> 
  select(fromidINS, toidINS, travel_time, euc)

mma <- tibble(fromidINS = names(masses_AMP$actifs), actifs = masses_AMP$actifs) |> 
  to_duckdb()
mme <- tibble(toidINS = names(masses_AMP$emplois), emplois = masses_AMP$emplois) |> 
  to_duckdb()
test5 <- distances.car |> 
  left_join(mma, by="fromidINS") |> 
  left_join(mme, by ="toidINS") |> 
  mutate(pf_ij = actifs*emplois*travel_time) |> 
  group_by(toidINS) |> 
  summarize(f_j = sum(pf_ij),
            d_j  = sum(travel_time*pf_ij)) |> 
  mutate(d_j = d_j/f_j) |> 
  collect() |> 
  left_join(c200ze|> select(toidINS = idINS), by="toidINS") |> 
  st_as_sf()

# check -------------------
if(check) {
  com_ar <- qs::qread(communes_ar_file)
  library(tmap)
  COMs |> 
    group_by(COMMUNE) |> 
    summarize(actifs = sum(actifs), fuites = sum(fuites)) |>
    left_join(tibble(flux  = rowSums(meaps.c), 
                     COMMUNE = as.integer(names(rowSums(meaps.c)))), by = "COMMUNE") |> 
    mutate(r = flux/actifs, COMMUNE = as.character(COMMUNE)) |> 
    left_join(com_ar |> select(INSEE_COM), by = c('COMMUNE' = 'INSEE_COM')) |> 
    st_as_sf() |> 
    tm_shape() + tm_borders() + tm_fill(col="r")
  
  DCLTs |> 
    mutate(DCLT = as.character(DCLT)) |> 
    group_by(DCLT) |> 
    summarize(emplois = sum(emplois)) |>
    left_join(tibble(flux  = colSums(meaps.c),
                     DCLT = names(colSums(meaps.c))), by = "DCLT") |> 
    mutate(r = flux/emplois) |> 
    left_join(com_ar |> select(INSEE_COM), by = c('DCLT' = 'INSEE_COM')) |> 
    st_as_sf() |> 
    filter(!st_is_empty(geometry)) |> 
    tm_shape() + tm_borders() + tm_fill(col="r")
  
  tibble(from = as.integer(froms), ai = matrixStats::rowSums2(meaps)) |> 
    left_join(c200ze |> select(from = idINS, act_mobpro)) |>
    mutate(r = ai/act_mobpro) |> 
    st_as_sf() |> 
    tm_shape() + tm_fill(col="r", style = "cont")
  tibble(to = as.integer(tos), ej = matrixStats::colSums2(meaps)) |> 
    left_join(c200ze |> select(to = idINS, emp_resident)) |>
    mutate(r = ej/emp_resident) |> 
    st_as_sf() |> 
    tm_shape() + tm_fill(col="r", style = "cont")
}
