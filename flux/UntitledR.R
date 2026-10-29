tic()
c200ze <- qs::qread(c200ze_file) |> 
  st_drop_geometry() |>
  select(idINS, ind, com, scot) |> 
  left_join(communes, join_by(com)) |> 
  filter(scot) |> 
  to_duckdb(con = con)

tbl1 <- parquet2duckdb(
  con,  "trtage",
  parquet = "/tmp/dists/**/*.parquet",
  hive = "true")

transitage <- tbl(con, tbl1) |>
  filter(mode %in% c("transit", "car_dgr")) |> 
  select(fromidINS, toidINS, tt = travel_time, at = access_time, mode ) |> 
  semi_join(c200ze, join_by(fromidINS == idINS)) |> 
  pivot_wider(names_from = mode, values_from = c(tt, at)) |> 
  select(-at_car_dgr, at = at_transit) |> 
  mutate(
    at = at/60,
    transit = if_else(is.na(tt_transit), 120, tt_transit/60),
    car = tt_car_dgr/60,
    dt = (tt_transit - tt_car_dgr)/60,
    rdt = tt_transit / tt_car_dgr) |>
  transmute(
    fromidINS, toidINS,
    transitage = (at<=10) & (transit <= 45) & (dt <= 15 | rdt <= 1.25) ) 

tbl <- parquet2duckdb(
  con,
  table = "meaps",
  parquet = "/tmp/meaps/**/*.parquet",
  hive = "true")  

tbl(con, tbl) |>
  mutate(d = round(distance/1000,1)) |>
  filter(d<=50, d >  0) |> 
  group_by(COMMUNE) |> 
  left_join(transitage, join_by(fromidINS, toidINS)) |> 
  duckdb2parquet("/tmp/joined", by = "COMMUNE")
toc()