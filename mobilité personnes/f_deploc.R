deploc_densite <- function(data, reg="all", K=128, label_reg = NULL,
                           var = "DENSITECOM_RES", out = c("km", "tr", "dtrj"),
                           label = "", labels = NULL) {
  vstat <- "kmind"
  scale <- 1
  title <- "Km par adultes par an"
  
  if("all"%in%reg) {
    region <- label_reg %||% "France entière"
    data <- data
  }
  else {
    data <- data |> filter(NUTS_ORI %in% reg)
    region <- data |>
      distinct(REG_ORI) |>
      pull(REG_ORI) |>
      str_c(collapse = ", ")
  }
  data <- data |> 
    mutate(dist = MDISTTOT_fin )  |> 
    mutate(motifbin = if_else(motif_principal=="travail",
                              "travail",
                              "autre"),
           motifbin = factor(motifbin,
                             c("travail","autre"))) 
  
  nobs <- nrow(data)
  individus <- data |>  
    distinct(IDENT_IND, pond_indC) |>
    summarize(ind = sum(pond_indC)) |> 
    pull(ind)
  adultes <- data |> 
    filter(AGE>=18) |> 
    distinct(IDENT_IND, pond_indC) |>  
    summarize(ind = sum(pond_indC)) |>
    pull(ind)
  
  calc <- furrr::future_map_dfr(1:K, ~{
    data.l <- data |> 
      slice_sample(prop=1, replace=TRUE)
    
    adultes_dens <- data.l |> 
      filter(AGE>=18) |> 
      distinct(IDENT_IND, pond_indC, across(all_of(var))) |> 
      group_by(across(all_of(var))) |> 
      summarize(ind = sum(pond_indC)) |> 
      pivot_wider(names_from = all_of(var), values_from = ind) |> 
      rowwise() |> 
      mutate(total = sum(c_across(everything()))) |> 
      pivot_longer(cols = everything(), 
                   names_to = "densite", 
                   values_to = "adultes")
    
    acto_dens <- data.l |> 
      filter(AGE>=18, ACTOCCUP==1) |> 
      distinct(IDENT_IND, pond_indC, across(all_of(var))) |>
      group_by(across(all_of(var))) |> 
      summarize(ind = sum(pond_indC)) |> 
      pivot_wider(names_from = all_of(var), values_from = ind) |> 
      rowwise() |> 
      mutate(total = sum(c_across(everything()))) |> 
      pivot_longer(cols = everything(), names_to = "densite", values_to = "actif_occup")
    
    km_dens <- data.l |> 
      filter(mode=="car") |> 
      group_by(across(all_of(var)), motifbin) |>
      summarize(
        kmvoy = 52*sum(dist*POND_JOUR),
        trajets = 52*sum(POND_JOUR),
        .groups = "drop") |> 
      pivot_wider(id_cols = motifbin,
                  names_from = all_of(var),
                  values_from = c(kmvoy,trajets),
                  values_fill = 0) |>
      rowwise() |> 
      mutate(kmvoy_total = sum(c_across(starts_with("kmvoy"))),
             trajets_total = sum(c_across(starts_with("trajets")))) |>
      pivot_longer(cols = -motifbin, values_to = "value", names_to = c("var", "densite"), names_sep = "_") |> 
      pivot_wider(names_from = motifbin, values_from = value) |> 
      mutate(total = autre+travail) 
    
    km_ad <- km_dens |> 
      filter(var == "kmvoy") |> 
      left_join(adultes_dens, by="densite") |> 
      transmute(densite, var="kmad", across(c(travail, autre, total), ~.x/adultes))
    
    km_acto <-  km_dens |> 
      filter(var == "kmvoy") |> 
      left_join(acto_dens, by="densite") |> 
      transmute(densite, var="kmac", across(c(travail), ~.x/actif_occup))
    
    trajets_ad <- km_dens |> 
      filter(var == "trajets") |> 
      left_join(adultes_dens, by="densite") |> 
      transmute(densite, var="trad", across(c(travail, autre, total), ~.x/adultes)) 
    
    trajets_acto <- km_dens |> 
      filter(var == "trajets") |> 
      left_join(acto_dens, by="densite") |> 
      transmute(densite, var = "trac", across(c(travail), ~.x/actif_occup))
    
    distrj_ad <- left_join(km_ad, trajets_ad, by="densite", suffix = c(".km", ".trj")) |> 
      mutate(travail = travail.km/travail.trj,
             autre = autre.km/autre.trj,
             total = total.km/total.trj) |> 
      select(densite, travail, autre, total) |> 
      mutate(var = "dtrjad")
    
    distrj_acto <- left_join(km_acto, trajets_acto, by="densite", suffix = c(".km", ".trj")) |> 
      mutate(travail = travail.km/travail.trj) |> 
      select(densite, travail) |> 
      mutate(var = "dtrjac")
    
    bind_rows(km_ad, km_acto, trajets_ad, trajets_acto, distrj_ad, distrj_acto) |> 
      mutate(s = .x)
  }, .options = furrr::furrr_options(seed = TRUE))
  
  d1 <- first(unique(calc$densite))
  
  calc_r <- calc |> 
    group_by(var, s) |> 
    mutate(across(
      c(travail, autre, total), ~.x/.x[densite==d1])) |> 
    ungroup() |> 
    mutate(var = str_c(var, "_r"))
  
  calc <- calc |>
    bind_rows(calc_r) |> 
    group_by(densite, var) |> 
    reframe(
      across(
        c(travail, total, autre),
        ~quantile(.x, probs = c(0.025, .5, 0.975), na.rm=TRUE)),
      p = c(0.025, .5, 0.975)) 
  
  if(!is.null(labels)) calc <- calc |> 
    mutate(densite = factor(densite, unique(densite), c(labels, "total")))
  
  
  ff <- function(x, n) format(x, digits=n, scientific=FALSE , big.mark=" ") 
  
  ts <- map(out, ~{
    vars <- str_c(.x, c("ac", "ad", "ac_r", "ad_r"))
    subtitle <- case_match(.x,
                           "km" ~ "km par personne par an",
                           "tr" ~ "trajets par personne par an",
                           "dtrj" ~ "distance par trajet par an")
    calc |> 
      filter(var%in%vars) |>
      mutate(var = str_remove(var, .x)) |> 
      pivot_wider(names_from = var, values_from = c(travail, autre, total)) |>
      select(-autre_ac, -total_ac, -autre_ac_r, -total_ac_r) |> 
      relocate(densite, 
               travail_ac, travail_ac_r, 
               travail=travail_ad, travail_r = travail_ad_r, 
               autre = autre_ad, autre_r=autre_ad_r, 
               total = total_ad, total_r= total_ad_r) |> 
      group_by(densite) |> 
      summarize(
        across(-p, 
               ~ str_c(ff(.x[p==0.5], 3),
                       "<br><small><small>[",
                       ff(.x[p==0.025], 2), ", ", 
                       ff(.x[p==0.975], 2), "]</small></small>"))) |>
      gt::gt() |> 
      gt::tab_header(title = region ,
                     subtitle = subtitle) |> 
      gt::cols_label(travail_ac = "travail",
                     densite = label) |>
      gt::tab_spanner(label = "par actif",
                      columns = c(travail_ac, travail_ac_r)) |>
      gt::tab_spanner(label = "par adulte",
                      columns = c(travail, travail_r, autre, autre_r,
                                  total, total_r)) |> 
      gt::fmt_markdown() |> 
      gt::cols_label(ends_with("_r") ~ "") |> 
      gt::cols_align(densite, align="left") |> 
      gt::cols_align(-densite, align="center") |>
      gt::tab_source_note(gt::md(
        "Source : EMP 2019,<br>
      {f2si2(adultes)} adultes (AGE>=18) dans la zone<br>
      {f2si2(nobs)} observations dans EMP19<br>
      {K} répétitions de rééchantillonage<br>
      entre crochets : intervalle de confiance à 95%" |> glue::glue()))
  })
  
  walk(ts, ~print(gt::as_raw_html(.x)))
  invisible(ts)
}