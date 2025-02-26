deploc_densite_2 <- function(data, reg="all", K=512, label_reg = NULL,
                             mode = c("car", "walk", "transit", "bike"),
                             var = "DENSITECOM_RES", out = c("km", "tr", "dtrj"),
                             label = "", labels = NULL,
                             source = "EMP 2019",
                             titre = "France entière",
                             soustitre = NULL) {
  vstat <- "kmind"
  scale <- 1
  title <- "Km par adultes par an"
  
  if("all"%in%reg) {
    region <- label_reg %||% titre
    data <- data
  }
  else {
    data <- data |> filter(NUTS_ORI %in% reg)
    region <- data |>
      distinct(REG_ORI) |>
      pull(REG_ORI) |>
      str_c(collapse = ", ")
  }
  # mutate(motifbin = if_else(motif_principal=="travail",
  #                           "travail",
  #                           "autre"),
  #        motifbin = factor(motifbin,
  #                          c("travail","autre"))) 
  nobs <- nrow(data)
  individus <- data |>  
    summarize(ind = sum(pond_indC)) |> 
    pull(ind)
  adultes <- data |> 
    filter(AGE>=18) |> 
    summarize(ind = sum(pond_indC)) |>
    pull(ind)
  actifs <- data |> 
    filter(AGE>=18, ACTOCCUP==1) |> 
    summarize(ind = sum(pond_indC)) |>
    pull(ind)

  calc <- furrr::future_map_dfr(1:K, ~{
    smp <- sample(1:nrow(data), size = nrow(data), replace = TRUE, prob = data$pond_indC)
    data.l <- data |> 
      slice(smp) |> 
      mutate(
        pij = POND_JOUR/pond_indC,
        pond_indC = adultes/nrow(data),
        POND_JOUR = pond_indC*pij)
    
    adultes_dens <- data.l |> 
      group_by(across(all_of(var))) |> 
      summarize(ind = sum(pond_indC),
                obs = n()) |> 
      pivot_wider(names_from = all_of(var), values_from = c(ind, obs)) |> 
      rowwise() |> 
      mutate(ind_total = sum(c_across(starts_with("ind"))),
             obs_total = sum(c_across(starts_with("obs")))) |> 
      pivot_longer(cols = everything(), 
                   names_to = "vvv", 
                   values_to = "values") |> 
      separate(vvv, into = c("type","vv"), sep = "_") |> 
      pivot_wider(names_from = type, values_from = values) |> 
      rename(adultes = ind)
    
    acto_dens <- data.l |> 
      filter(AGE>=18, ACTOCCUP==1) |> 
      group_by(across(all_of(var))) |> 
      summarize(ind = sum(pond_indC)) |> 
      pivot_wider(names_from = all_of(var), values_from = ind) |> 
      rowwise() |> 
      mutate(total = sum(c_across(everything()))) |> 
      pivot_longer(cols = everything(), 
                   names_to = "vv",
                   values_to = "actif_occup")
    
    ind_aggr <- data.l |> 
      group_by(across(all_of(var))) |>
      summarize(
        across(starts_with("distance_"), ~52*sum(.x*POND_JOUR)),
        across(starts_with("nb_boucles_"), ~52*sum(.x*POND_JOUR))) |> 
      pivot_longer(cols = -all_of(var)) |> 
      mutate(name = str_replace(name, "nb_boucles_", "boucles_")) |> 
      separate(name, into = c("measure","mode", "motif")) |> 
      filter(mode %in% !!mode) |> 
      group_by(across(all_of(var)), measure, motif) |> 
      summarise(value = sum(value), .groups = "drop") |> 
      pivot_wider(names_from = motif, values_from = value) |> 
      mutate(total = autres+travail+etudes+courses) |> 
      relocate(travail, etudes, courses, autres, total, .after = measure) |> 
      mutate(vv = as.character(.data[[!!var]])) |> 
      select(-all_of(var))
    
    ind_totaux <- ind_aggr |> 
      group_by(measure) |> 
      summarize(across(c(travail, etudes, courses, autres, total), sum)) |> 
      mutate(vv = "total")
    
    ind_aggr <- ind_aggr |> 
      bind_rows(ind_totaux)
    
    km_ad <- ind_aggr  |> 
      filter(measure == "distance") |> 
      left_join(adultes_dens, by="vv") |> 
      transmute(vv, variable="kmad", across(c(travail, autres, courses, etudes, total), ~.x/adultes))
    
    km_acto <-  ind_aggr |> 
      filter(measure == "distance") |> 
      left_join(acto_dens, by="vv") |> 
      transmute(vv, variable="kmac", across(c(travail), ~.x/actif_occup))
    
    trajets_ad <- ind_aggr |> 
      filter(measure == "boucles") |> 
      left_join(adultes_dens, by="vv") |> 
      transmute(vv, variable="trad", across(c(travail, autres, courses, etudes, total), ~.x/adultes)) 
    
    trajets_acto <- ind_aggr |> 
      filter(measure == "boucles") |> 
      left_join(acto_dens, by="vv") |> 
      transmute(vv, variable = "trac", across(c(travail), ~.x/actif_occup))
    
    distrj_ad <- left_join(km_ad, trajets_ad, by="vv", suffix = c(".km", ".trj")) |> 
      mutate(travail = travail.km/travail.trj,
             autres = autres.km/autres.trj,
             etudes = etudes.km/etudes.trj,
             courses = courses.km/courses.trj,
             total = total.km/total.trj) |> 
      select(vv, travail, autres, courses, etudes, total) |> 
      mutate(variable = "dtrjad")
    
    distrj_acto <- left_join(km_acto, trajets_acto, by="vv", suffix = c(".km", ".trj")) |> 
      mutate(travail = travail.km/travail.trj) |> 
      select(vv, travail) |> 
      mutate(variable = "dtrjac")
    
    bind_rows(km_ad, km_acto, trajets_ad, trajets_acto, distrj_ad, distrj_acto) |> 
      left_join(adultes_dens, by  ="vv") |> 
      mutate(s = .x)
  }, .options = furrr::furrr_options(seed = TRUE))
  
  d1 <- first(unique(calc$vv))
  
  calc_r <- calc |> 
    group_by(variable, s) |> 
    mutate(across(
      c(travail, autres, courses, etudes, total), ~.x/.x[vv=="total"])) |> 
    ungroup() |> 
    mutate(variable = str_c(variable, "_r"))
  
  calc_q <- calc |>
    bind_rows(calc_r) |> 
    group_by(vv, variable) |> 
    reframe(
      across(
        c(travail, total, autres, courses, etudes, adultes, obs),
        ~quantile(.x, probs = c(0.025, .5, 0.975), na.rm=TRUE)),
      p = c(0.025, .5, 0.975)) 
  
  if(!is.null(labels)) calc_q <- calc_q |> 
    mutate(vv = factor(vv, unique(vv), c(labels, "total")))
  
  
  ff <- function(x) {
    map_chr(x, ~{
      if(.x>=100)
        str_c(format(.x/1000, digits=3, scientific=FALSE , big.mark=" "), "k")
      else if(.x<100)
        format(.x, digits=2, scientific=FALSE , big.mark=" ") 
    })
  }
  
  ts <- map(out, ~{
    vars <- str_c(.x, c("ac", "ad", "ac_r", "ad_r"))
    subtitle <- case_match(
      .x,
      "km" ~ "km par personne par an",
      "tr" ~ "trajets par personne par an",
      "dtrj" ~ "distance par trajet")
    if(!is.null(soustitre))
      subtitle <- str_c(subtitle, soustitre)
    data <- calc_q |> 
      select(-adultes, -obs) |> 
      filter(variable%in%vars) |>
      mutate(variable = str_remove(variable, .x),
             grp = ifelse(str_detect(variable, "_r$"), "relatif", "km"),
             pa = str_remove(variable, "_r")) |> 
      pivot_longer(cols = c(travail, total, autres, courses, etudes)) |>
      drop_na(value) |> 
      mutate(name = str_c(name, "_", pa)) |> 
      select(-pa, -variable) |> 
      pivot_wider(names_from = name, 
                  values_from = value) |> 
      relocate(vv, 
               travail_ac, 
               travail=travail_ad,  
               etudes = etudes_ad,  
               courses = courses_ad, 
               autres = autres_ad,  
               total = total_ad) |> 
      group_by(grp, vv) |> 
      summarize(
        across(-p, 
               ~ str_c(ff(.x[p==0.5]),
                       "<br><small><small>[",
                       ff(.x[p==0.025]), ", ", 
                       ff(.x[p==0.975]), "]</small></small>")),
        .groups = "drop")
    table <- data |>
      gt::gt(groupname_col = "grp") |> 
      gt::tab_header(title = region ,
                     subtitle = subtitle) |> 
      gt::cols_label(travail_ac = "travail",
                     vv = label) |>
      gt::tab_spanner(label = "par actif",
                      columns = c(travail_ac)) |>
      gt::tab_spanner(label = "par adulte",
                      columns = c(travail,
                                  etudes,
                                  courses, 
                                  autres, 
                                  total)) |> 
      gt::fmt_markdown() |> 
      gt::cols_label(ends_with("_r") ~ "") |> 
      gt::cols_align(vv, align="left") |> 
      gt::cols_align(-vv, align="center") |>
      gt::tab_options(
        data_row.padding = gt::px(2) ) |>
      gt::opt_css("br { line-height: 50%;} small {line-height: 50%;}") |> 
      gt::tab_source_note(gt::md(
        "*Source* : {source},<br>
        *Note* : {f2si2(adultes)} adultes (AGE>=18) dans la zone ; 
        {f2si2(nobs)} observations dans {source} ;
        {K} répétitions de rééchantillonage ;
        entre crochets : intervalle de confiance à 95%" |> glue::glue()))
    list(table = table, cells = data)
  })
  
  ts <- purrr::transpose(ts)
  names(ts$cells) <- out
  names(ts$table) <- out
  ts$raw <- calc_q
  ts$full <- calc
  invisible(ts)
}
