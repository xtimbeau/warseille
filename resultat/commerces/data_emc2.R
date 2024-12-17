ind_emc2 <- bd_read("ind_emc2")

labels <- set_names(c("Alimentaire", "Com. non alim.", "Bars, ...", "Santé humaine", 
                      "Densité", "Densité pondérée"),
                    c("alim","comm","sortie","sante", "dens", "pwd"))

data_emc2 <- ind_emc2 |> 
  pivot_longer(cols = c(alim, comm, sortie, sante, dens, pwd), names_to = "type", values_to = "se") |> 
  mutate(type = factor(type, c("alim","comm","sortie","sante", "dens", "pwd"))) |> 
  group_by(type) |> 
  mutate(
    type_lab = labels[type],
    q = santoku::chop_deciles(se, weights = poids_t, extend = FALSE, labels = str_c("d", 1:10)) ) 

return(list(d=data_emc2, l = labels))