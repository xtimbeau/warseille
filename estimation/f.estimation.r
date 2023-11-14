ITOR <- function(mob_0, mutation, name, par, seuil_dflux2 = 1, steps = 50) {
  ors <- mob_0 |> 
    mutate(or.0 = 1) |> 
    rename(thor.0 = or,
           flux.0 = flux) |> 
    relocate(COMMUNE, DCLT, mobpro, cum, flux.0)
  
  cli_progress_bar(
    "ITOR, d(flux^2)",
    format = "ITOR - {name} {pb_spin} itération {pb_current} ({round(1/pb_rate_raw)}s/iter) df2: {signif(dor2, 4)} en {pb_elapsed}")
  i <- 1
  dor2 <- 2*seuil_dflux2
  while(i<5 | (dor2>seuil_dflux2 & i<=steps)) {
    ors <- mutation(ors, i) 
    
    odds_i <- ors |> 
      select(COMMUNE, DCLT, or = any_of(glue("or.{i}"))) |>
      creer_matrice_odds()
    
    MOD_i <- do.call(
      meaps_multishuf, 
      append(par, list(modds = odds_i, progress = FALSE)))
    communal <- communaliser1(list(emps=MOD_i)) 
    names(communal) <- c("COMMUNE", "DCLT", 
                         str_c(c("flux", "mobpro", "thor"),".", i))
    ors <- ors |> 
      left_join(
        communal |> select(-starts_with("mobpro")), 
        by=c("COMMUNE", "DCLT"))
    
    dor2 <- sqrt( mean((ors[[glue("flux.{i}")]]-ors[[glue("flux.{i-1}")]])^2))
    i <- i + 1
    cli_progress_update()
    gc()
  }
  cli_progress_done()
  
  res <- ors |> 
    pivot_longer(cols = starts_with(c("or", "thor", "flux")), 
                 names_to = c(".value", "set"), 
                 names_pattern= "([a-z]+)\\.([0-9]+)") |> 
    left_join(dist.com, by=c("COMMUNE", "DCLT")) |> 
    group_by(COMMUNE, DCLT) |> 
    mutate(
      set = as.numeric(set),
      estime = or!=or[set==0],
      alg = !!name) |> 
    ungroup()
  
  stats <- res |>
    group_by(set) |> 
    comparer_meaps_bygrp()
  
  bf_set <- stats |> filter(set>=5) |> filter(r2kl == max(r2kl)) |> pull(set)
  bf <- res |> 
    filter(set == bf_set) |> 
    mutate(estime = (or != (res |> filter(set==0) |> pull(or))),
           n_est = sum(estime))
  cli_inform(
    "ITOR - {name} {bf_set} itérations r2kl={signif(max(stats$r2kl),3)} n_est={bf$n_est[[1]]}"
  )
  return(list(bf = bf, stats = stats, itors = res))
}

# on découpe les actifs en paquet en des paquets plus petits

emiette <- function(les_actifs, nshuf = 256, seuil=40, var = "actifs", weighted=TRUE) {
  if(is_tibble(les_actifs))
    act <- les_actifs |> 
      pull(var)
  if(is_vector(les_actifs))
    act <- les_actifs
  freq <- act %/% seuil + 1
  set <- NULL
  w <- NULL
  for(i in 1:length(freq)) {
    set <- c(set, rep(i, freq[i]))
    w <- c(w, rep(act[i]/freq[i], freq[i]))
  }
  shuf <- matrix(NA, ncol = sum(freq), nrow = nshuf)
  if(weighted==FALSE)
    w <- NULL
  for(i in 1:nshuf) 
    shuf[i, ] <- sample(set, sum(freq), prob = w, replace=FALSE)
  return(shuf)
}