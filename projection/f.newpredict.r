# passe d'un facteur à une matrice de design
creer_design <- function(x, nom = names(x), levels = levels(x)) {
  
  res <- matrix(nrow = length(x), ncol = length(levels))
  colnames(res) <- paste0(nom, levels)
  
  for (i in seq_along(levels)) res[, i]  <-  as.numeric(x == levels[i])
  
  res
}


# Predict sur des données idx
newpredict <- function(model, newdata) {
  
  les_cols <- model$coefficients |> names()
  les_choix <- model$freq |> names()
  les_coefs <- model$coefficients |> as.numeric()
  les_vars <- terms(model$model) |> attr("term.labels") |> setdiff(paste0("id", 1:10))
  
  indices_communs <- which(!str_detect(les_cols, paste0(":", les_choix, collapse = "|")))
  indices_specifiques <- map(les_choix, ~ which(str_detect(les_cols, paste0(":", .x))))
  
  choix_ref <- which(map_int(indices_specifiques, length) == 0)
  
  les_noms <- map_chr(les_cols, ~ ifelse (str_detect(.x, ":"), 
                                          str_extract(.x, ".+(?=:)"),
                                          .x))
  
  les_types <- map(les_vars, function(v) {
    map_chr(les_noms, ~ {
      if (.x %in% les_vars) NA_character_ else str_extract(.x, paste0("(?<=^", v,").+")) 
    })
  })
  
  les_facteurs <- map_lgl(les_types, ~ !all(is.na(.x)))
  les_labels <- map(les_types, ~ unique(.x) |> discard(is.na))[les_facteurs]
  les_facteurs <- les_vars[les_facteurs]
  names(les_labels) <- les_facteurs
  
  les_generalistes <- map_lgl(les_vars, function(v) {
    if (v %in% les_facteurs) {
      filtre_noms <- paste0(v, les_labels[[v]], ":", collapse = "|")
      !any(str_detect(les_cols, filtre_noms))
    } else {
      !any(str_detect(les_cols, paste0(v, ":")))
    }
  })
  
  les_generalistes <- les_vars[les_generalistes]
  
  newtib <- as_tibble(newdata)
  
  mat <- matrix(nrow = nrow(newdata), ncol = length(les_choix) - 1)
  colnames(mat) <- les_cols[str_detect(les_cols, "(Intercept)")]
  
  les_choix_noref <- les_choix[-choix_ref]
  
  for (i in seq_along(les_choix_noref)) mat[, i] <- as.numeric(newdata$idx$id2 == les_choix_noref[i])
  
  for (v in les_vars) {
    
    if (v %in% les_facteurs) {
      if (v %in% les_generalistes) {
        mat <- cbind(mat, creer_design(newtib |> pull(.data[[v]]), 
                                       nom = v,
                                       levels = les_labels[[v]]))
      } else {
        mat_temp <- creer_design(newtib |> pull(.data[[v]]), 
                                 nom = v,
                                 levels = les_labels[[v]])
        for (i in 1:ncol(mat_temp)) 
          for (j in seq_along(les_choix_noref)) {
            mat_temp2 <- mat_temp[, i] * mat[, j] |> as.matrix(ncol = 1)
            colnames(mat_temp2) <- paste0(colnames(mat_temp)[i], ":", les_choix_noref[j])
            mat <- cbind(mat, mat_temp2)}
        
      }
    } else {
      if (v %in% les_generalistes) {
        mat_temp <- newtib|> pull(.data[[v]]) |> as.matrix(ncol = 1)
        colnames(mat_temp) <- v
        mat <- cbind(mat, mat_temp)
      } else {
        mat_temp <- newtib|> pull(.data[[v]]) |> as.matrix(ncol = 1)
        for (j in seq_along(les_choix_noref)) {
          mat_temp2 <- mat_temp * mat[, j] |> as.matrix(ncol = 1)
          colnames(mat_temp2) <- paste0(v, ":", les_choix_noref[j])
          mat <- cbind(mat, mat_temp2)
        }
      }
    }
  }
  
  # les utilités
  mat <- mat %*% les_coefs |> 
    matrix(ncol = length(les_choix), byrow = TRUE)
  colnames(mat) <- les_choix
  
  # les probabilités du RUM dans le cas simple d'un MNL
  mat <- exp(mat)
  
  return(mat / rowSums(mat))
}

# Predict sur des données sans idx (avant le reshape)
newpredict_widedata <- function(model, newdata, sep = "_") {
  
  les_cols <- model$coefficients |> names()
  les_choix <- model$freq |> names()
  les_coefs <- model$coefficients |> as.numeric()
  les_vars <- terms(model$model) |> attr("term.labels") |> setdiff(paste0("id", 1:10))
  
  indices_communs <- which(!str_detect(les_cols, paste0(":", les_choix, collapse = "|")))
  indices_specifiques <- map(les_choix, ~ which(str_detect(les_cols, paste0(":", .x))))
  
  choix_ref <- which(map_int(indices_specifiques, length) == 0)
  
  les_noms <- map_chr(les_cols, ~ ifelse (str_detect(.x, ":"), 
                                          str_extract(.x, ".+(?=:)"),
                                          .x))
  
  les_types <- map(les_vars, function(v) {
    map_chr(les_noms, ~ {
      if (.x %in% les_vars) NA_character_ else str_extract(.x, paste0("(?<=^", v,").+")) 
    })
  })
  
  les_facteurs <- map_lgl(les_types, ~ !all(is.na(.x)))
  les_labels <- map(les_types, ~ unique(.x) |> discard(is.na))[les_facteurs]
  les_facteurs <- les_vars[les_facteurs]
  names(les_labels) <- les_facteurs
  
  les_generalistes <- map_lgl(les_vars, function(v) {
    if (v %in% les_facteurs) {
      filtre_noms <- paste0(v, les_labels[[v]], ":", collapse = "|")
      !any(str_detect(les_cols, filtre_noms))
    } else {
      !any(str_detect(les_cols, paste0(v, ":")))
    }
  })
  
  les_generalistes <- les_vars[les_generalistes]
  les_specifiques <- setdiff(les_vars, les_generalistes)
  
  les_fac_spe <- intersect(les_specifiques, les_facteurs)
  les_num_spe <- setdiff(les_specifiques, les_facteurs)
  les_fac_gen <- intersect(les_generalistes, les_facteurs)
  les_num_gen <- setdiff(les_generalistes, les_facteurs)
  
  les_choix_noref <- les_choix[-choix_ref]
  
  utilites_specifiques <- map(les_choix_noref, \(mod) {
    
    contrib_fac_spe <- map(les_fac_spe, \(fac) {
      icoefs <- which(str_detect(names(model$coefficients), paste0(fac, ".+:", mod)))
      ilabs <- str_extract(names(model$coefficients)[icoefs], paste0("(?<=", fac, ").+(?=:", mod, ")"))
      
      left_join(newdata |> transmute("{fac}" := as.character(.data[[fac]])),
                tibble("{fac}" := ilabs, value = les_coefs[icoefs]),
                by = fac) |> 
        transmute("{fac}" := replace_na(value, 0))
      
    })
    
    contrib_intercept <- which(names(model$coefficients) == paste0("(Intercept):", mod))
    contrib_intercept <- les_coefs[contrib_intercept]
    
    contrib_num_spe <- map(les_num_spe, \(x) {
      icoefs <- which(str_detect(names(model$coefficients), paste0(x, ":", mod)))
      newdata |> 
        transmute("{x}" := .data[[x]] * les_coefs[icoefs])
    })
    
    total <- append(contrib_fac_spe, contrib_num_spe) |> list_cbind() |> rowSums() 
    
    tibble("{mod}" := total + contrib_intercept)
    
  }) |> list_cbind()
  
  utilites_specifiques <- utilites_specifiques |> 
    mutate("{les_choix[choix_ref]}" := 0) 
  
  utilites_generalistes <- map(les_choix, \(mod) {
    
    contrib_fac_gen <- map(les_fac_gen, \(fac) {
      icoefs <- which(str_detect(names(model$coefficients), fac))
      ilabs <- str_extract(names(model$coefficients)[icoefs], paste0("(?<=", fac, ").+"))
      var_mod <- paste0(fac, sep, mod)
      left_join(newdata |> transmute("{var_mod}" := as.character(.data[[var_mod]])),
                tibble("{var_mod}" := ilabs, value = les_coefs[icoefs]),
                by = var_mod) |> 
        transmute("{fac}" := replace_na(value, 0))
    }) 
    
    contrib_num_gen <-  map(les_num_gen, \(x) {
      icoefs <- which(names(model$coefficients) == x)
      var_mod <- paste0(x, sep, mod)
      newdata |> 
        transmute("{x}" := .data[[var_mod]] * les_coefs[icoefs])
    })
    
    total <- append(contrib_fac_gen, contrib_num_gen) |> list_cbind() |> rowSums()
    tibble("{mod}" := total)
    
  }) |> list_cbind()
  
  utilites_specifiques <- utilites_specifiques |> relocate(all_of(les_choix)) |> as.matrix()
  utilites_generalistes <- utilites_generalistes |> relocate(all_of(les_choix)) |> as.matrix()
  
  utilites <- (utilites_specifiques + utilites_generalistes) |> exp()
  
  
  # les probabilités du RUM dans le cas simple d'un MNL
  
  return(utilites / rowSums(utilites))
}
