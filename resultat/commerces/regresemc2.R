data_emc2 <- source_data("commerces/data_emc2.R")

labels <- data_emc2$l

n_min <- 10
fmrl <- "log(1+d_car) ~  q + log(rev_uc) + actoccup + enfants"
fmrl_alt <- "log(1+d_car) ~  log(se) + log(rev_uc) + actoccup + enfants"
res <- map_dfr(c("alim", "comm", "sortie", "sante", "dens", "pwd"), \(.mod) {
  mod <- lm( fmrl ,data = data_emc2$d |> filter(type == .mod, n>n_min)) 
  mod_alt <- lm( fmrl_alt ,data = data_emc2$d |> filter(type == .mod, n>n_min)) 
  r2 <- broom::glance(mod) |> 
    select(r.squared, adj.r.squared, nobs) |> 
    mutate(mod = labels[.mod])
  coef <- broom::tidy(mod) |> 
    select(term, e = estimate, se = std.error, p = p.value) |> 
    pivot_wider(names_from = term, values_from = c(e, se, p)) |> 
    rename(c = `e_(Intercept)`,
           se_c = `se_(Intercept)`,
           p_c = `p_(Intercept)`,
           rev_uc = `e_log(rev_uc)`,
           se_rev_uc = `se_log(rev_uc)`,
           p_rev_uc = `p_log(rev_uc)`) |> 
    rename_with(.fn = ~str_remove(.x, "^e_"))
  alt <- broom::tidy(mod_alt) |> 
    filter(term == "log(se)")
  alt <- tibble(se = alt$estimate,
                se_se = alt$std.error,
                p_se = alt$p.value)
  
  bind_cols(r2, coef, alt)
})

fmt_c <- function(s, se, p) {
  stars <- ifelse(p<=0.01, "***", ifelse(p<=0.05, "**", ifelse(p<=.1, "*", "")))
  str_c(round(s, 2), stars, "<br>(", ifelse(se==0, "-", round(se,2)), ")")
}

tbl_data <- res |> 
  mutate(
    rev_uc = fmt_c(rev_uc, se_rev_uc, p_rev_uc),
    enfants = fmt_c(enfants, se_enfants, p_enfants),
    actoccup = fmt_c(actoccup, se_actoccup, p_actoccup),
    se = fmt_c(se, se_se, p_se),
    c = fmt_c(c, se_c, p_c)) |> 
  select(-c(se_rev_uc, p_rev_uc, se_enfants, p_enfants, se_actoccup,
            p_actoccup, se_c, p_c, se_se, p_se)) |> 
  mutate(
    across(starts_with("qd"), 
           ~ fmt_c(.x, res[[str_c("se_", cur_column())]], res[[str_c("p_", cur_column())]]), 
           .names = "c{.col}")) |>
  mutate(cqd1 = " 0<br>ref", qd1 = 0) |> 
  relocate(cqd1, .before = cqd2) |> 
  relocate(qd1, .before = qd2) |> 
  select(mod, r.squared, adj.r.squared, nobs, rev_uc, enfants, actoccup, se, c,
         starts_with("cqd"), starts_with("qd")) |> 
  rename_with(.cols = starts_with("cqd"), .fn = ~str_remove(.x, "cq") )

dmc2 <- data_emc2$d |> 
  group_by(q, type) |>
  drop_na(q) |>
  summarize(m = mean(d_car), se = sd(d_car)) |>
  group_by(type) |>
  mutate( m = m/m[q == "d1"]) |> 
  select(mod = type, dec = q, q_data = m, se_data = se) |> 
  mutate(mod = factor(labels[mod], labels))

return(list(res = res, tbl = tbl_data, dmc2  = dmc2))
