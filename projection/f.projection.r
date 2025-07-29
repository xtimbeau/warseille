library(data.table)
#library(purrr)

# # sociodemo pour chaque carreau : population dans chaque categorie et moyenne pour les variables continues
# # distance les paires de carreaux avec la distance et les timetravel
# projeter <- function(sociodemo, distance, rum, trajets, detours) {
#   
#   newdata <- distance[sociodemo, on = c("fromidINS" = "idINS"), allow.cartesian = TRUE]
#   les_timetravel <- newdata |> names() |> purrr::keep(.p = \(x) grepl("^timetravel_"))
#   
#   pred_rum <- newpredict(rum, newdata = dfidx::dfidx(as.data.frame(newdata), 
#                                                      shape = "wide", 
#                                                      varying = les_timetravel, 
#                                                      sep = "_")) 
#   
#   multiplicateur <- predict(trajets, newdata) * predict(detours, newdata)
#   
#   pred_rum <- sweep(pred_rum, MARGIN = 1, STATS = multiplicateur, FUN = "*")
#   
#   cbind(newdata[, .(fromidINS, toidINS, distance, poids)], pred_rum)
# }

# Chaque table définit une marge = composition de chaque carreau selon une variable ou un croisement de variables
# Les colonnes des tables sont imposées : idINS, var1, var2..., poids.
# Il doit y avoir une ligne par combinaison de modalités.
# La première table est la référence pour les totaux dans chacun des carreaux, en cas de discordance.
# Le poids final attribué est calculé sous l'hypothèse d'indépendance.
remplir_categories <- function(...){
  les_marges <- list(...)
  N <- length(les_marges)
  
  for (i in 1:N) {
    setDT(les_marges[[i]])
    les_marges[[i]][, poids := as.numeric(poids)]
    if (les_marges[[i]][, min(poids, na.rm = TRUE)] < 0) cli::cli_alert_info("Il y a des carreaux avec un nb d'individus négatif.")
    if (les_marges[[i]][, anyNA(poids)] == TRUE) cli::cli_alert_info("Il y a des carreaux avec des poids manquants.")
  }
  
  if (N == 1) {
    cli::cli_alert_info("Il y a une seule table !")
    return(les_marges[[1]])
  }
  
  les_totaux <- purrr::map(les_marges, \(x) x[, .(sum(poids)), by = "idINS"]) |> 
    purrr::reduce(\(x, y) x[y, on = "idINS"])
  
  # Vérification de la concordance des poids dans chacun des carreaux
  mat <- as.matrix(les_totaux[, -c(1,2)])
  diffmat <- sweep(mat, MARGIN = 1, STATS = as.matrix(les_totaux[, 2]), FUN = "-" ) |> 
    abs() |> colSums()
  tot_ref <- les_totaux[, 2] |> sum()
  
  ecart <- diffmat / tot_ref
  
  # Calage éventuel des poids
  for (i in 2:N) {
    if (ecart[i-1L] > 0) { 
      marge_tot <- les_marges[[1]][, .(idINS, poids)][, .(poids_tot = sum(poids)), by = .(idINS)]
      les_marges[[i]] <- merge(les_marges[[i]], 
                               marge_tot, 
                               by = "idINS")
      les_marges[[i]][, poids := poids / poids_tot * sum(poids), by = "idINS"]
      les_marges[[i]][, poids_tot := NULL]
    }
  }
  
  # Calcul des proportions
  les_vars <- purrr::map(les_marges, \(x) names(x) |> setdiff(c("idINS", "poids"))) |> 
    unlist()

  for (i in 1:N) {
    les_marges[[i]][, prop_tmp := poids / sum(poids), by = .(idINS)]
    setnames(les_marges[[i]], c("poids", "prop_tmp"), c(paste0(c("tmp_w", "prop_"), i)))
  }
  
  les_w <- paste0("tmp_w", 1:N)
  
  res <- purrr::reduce(les_marges, \(x, y) x[y, on = "idINS", allow.cartesian = TRUE])
  
  poids_final <- res[, .SD, .SDcols = c("tmp_w1", paste0("prop_", 2:N))] |> 
    as.matrix() |> 
    apply( MARGIN = 1, FUN = "cumprod")
  
  res[, poids := poids_final[N,]]
  
  les_vars <- c("idINS", les_vars, "poids")
  res[, .SD, .SDcols = les_vars]
  }
  
  

# groupe test

# tab1 <- expand.grid(idINS = 1:10, var1 = c("H", "F"), var2 = 1:3) |> as.data.table()
# tab1[, poids := rep(10, 60)]
# 
# tab2 <- data.table(idINS = rep(1:10, 2), var3 = c(rep(c("A", "B"), 5), rep(c("B", "A"), 5)))
# 
# tot <-  tab1[, .(tot = sum(poids)), by = "idINS"]
# tab2 <- tab2[tot, on = "idINS"]
# tab2[, poids := tot / 2][, tot := NULL]
# 
# tab3 <- data.table(idINS = c(rep(1:9, 2), 1, 1), var4 = rep(1, 20), poids = rpois(20, 15))

#remplir_categories(tab1, tab2, tab3)
