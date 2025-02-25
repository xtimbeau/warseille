library(tidyverse)

if (!file.exists("/tmp/k_deploc_public.csv")) {
  zip::unzip(
    zipfile = "/space_mounts/data/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
    exdir = "/tmp",
    files = "k_deploc_public.csv")
}
if (!file.exists("/tmp/k_individu_public.csv")) {
  zip::unzip(
    zipfile = "/space_mounts/data/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
    exdir = "/tmp",
    files = "k_individu_public.csv")
}
if (!file.exists("/tmp/tcm_men_public.csv")){
  zip::unzip(
    zipfile = "/space_mounts/data/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
    exdir = "/tmp",
    files = "tcm_men_public.csv")
}
if (!file.exists("/tmp/q_menage_public.csv")){
  zip::unzip(
    zipfile = "/space_mounts/data/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
    exdir = "/tmp",
    files = "q_menage_public.csv")
}
if (!file.exists("/tmp/q_voitvul_public.csv")){
  zip::unzip(
    zipfile = "/space_mounts/data/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
    exdir = "/tmp",
    files = "q_voitvul_public.csv")
}
if (!file.exists("/tmp/tcm_ind_kish_public.csv")){
  zip::unzip(
    zipfile = "/space_mounts/data/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
    exdir = "/tmp",
    files = "tcm_ind_kish_public.csv")
}

deploc <- read_delim("/tmp/k_deploc_public.csv", delim = ";", locale = locale("fr", encoding = "ISO-8859-1"))
# problems()
k_individu <- read_delim("/tmp/k_individu_public.csv", delim = ";", locale = locale("fr", encoding = "ISO-8859-1"))
# problems()
tcm_men <- read_delim("/tmp/tcm_men_public.csv", delim = ";", locale = locale("fr", encoding = "ISO-8859-1"))
# problems()
q_men <- read_delim("/tmp/q_menage_public.csv", delim = ";", locale = locale("fr", encoding = "ISO-8859-1"))

tcm_ind_kish <- read_delim("/tmp/tcm_ind_kish_public.csv", delim = ";", locale = locale("fr", encoding = "ISO-8859-1"))

tcm_ind_kish <- tcm_ind_kish |> 
  mutate(CS24 = as.integer(CS24))

q_voitvul <- read_delim("/tmp/q_voitvul_public.csv", delim = ";", locale = locale("fr", encoding = "ISO-8859-1"))

q_voitvul <- q_voitvul |> 
  select(IDENT_MEN, IDENT_NUMVEH, pond_veh, contains("energie"), KVKMV, KVKM1ANV) |> 
  nest(vehicules = - IDENT_MEN) |> 
  mutate(nb_voitures = map_int(vehicules, nrow))

sociodemo <- k_individu |> 
  select(IDENT_MEN, IDENT_IND, pond_indC, BTCSATISF) |> 
  left_join(select(
    tcm_men, 
    npers = NPERS, nenfants = NENFANTS, 
    ident_men, pond_menC, decile_rev, quartile_rev, TYPMEN5,
    decile_rev_uc, quartile_rev_uc, contains(c("RES", "res"))), 
    by = c("IDENT_MEN" = "ident_men")) |> 
  left_join(select(
    q_men,
    IDENT_MEN, BLOGDIST), by = "IDENT_MEN") |> 
  left_join(select(
    tcm_ind_kish,
    ident_ind, AGE, SEXE, ACTOCCUP, CS_ACT, CS24),
    by = c("IDENT_IND" = "ident_ind")) |> 
  left_join(q_voitvul, by = "IDENT_MEN") |> 
  mutate(nb_voitures = replace_na(nb_voitures, 0L))

deploc <- deploc |> full_join(sociodemo, by = c("IDENT_MEN", "IDENT_IND"))

# quick codebook
motifs <- tribble(~ code, ~ motif,
                  1.1, "Retour au domicile",
                  1.2, "Retour résidence occasionnelle",
                  1.3, "Retour au domicile de parents ou d’amis",
                  1.4, "Étudier (école, lycée, université)",
                  1.5, "Faire garder un enfant en bas âge",
                  2.1, "Se rendre dans une grande surface ou un centre commercial",
                  2.2, "Se rendre dans un centre de proximité, petit commerce",
                  3.1, "Soins médicaux ou personnels (médecin, coiffeur…)",
                  4.1, "Démarche administrative",
                  4.12, "Déchetterie",
                  5.1, "Visite à la famille",
                  5.2, "Visite à des amis",
                  6.1, "Accompagner quelqu’un à la gare, l’aéroport, au métro...",
                  6.2, "Accompagner quelqu’un à un autre endroit", 
                  6.3, "Aller chercher quelqu’un à la gare, à l’aéroport, au métro...", 
                  6.4, "Aller chercher quelqu’un à un autre endroit",
                  7.1, "Activité associative, cérémonie religieuse, réunion",
                  7.2, "Aller dans un centre de loisir, parc d’attraction, foire",
                  7.3, "Manger ou boire à l’extérieur du domicile",
                  7.4, "Visiter un monument ou un site historique",
                  7.5, "Voir un spectacle culturel ou sportif",
                  7.6, "Faire du sport",
                  7.7, "Se promener sans destination précise",
                  7.8, "Se rendre sur un lieu de promenade",
                  8.1, "Vacances hors résidence secondaire",
                  8.2, "Se rendre dans une résidence secondaire",
                  8.3, "Se rendre dans une résidence occasionnelle",
                  8.4, "Autres motifs personnels",
                  9.1, "Travailler dans son lieu fixe et habituel",
                  9.2, "Travailler en dehors d’un lieu fixe et habituel",
                  9.3, "Stages, conférence, congrès, formations, exposition",
                  9.4, "Tournées professionnelles (VRP) ou visites de patients",
                  9.5, "Autres motifs professionnels",
                  9999, NA_character_)

motifs2 <- tribble(~ code, ~ motif1,
                   1.1, "retour",
                   1.2, "retour",
                   1.3, "retour",
                   1.4, "etudes",
                   1.5, "soins",
                   2.1, "courses",
                   2.2, "courses",
                   3.1, "soins",
                   4.1, "demarches",
                   4.12, "demarches",
                   5.1, "visite",
                   5.2, "visite",
                   6.1, "accompagner",
                   6.2, "accompagner", 
                   6.3, "accompagner", 
                   6.4, "accompagner",
                   7.1, "loisirs",
                   7.2, "loisirs",
                   7.3, "loisirs",
                   7.4, "loisirs",
                   7.5, "loisirs",
                   7.6, "loisirs",
                   7.7, "loisirs",
                   7.8, "loisirs",
                   8.1, "vacances",
                   8.2, "vacances",
                   8.3, "vacances",
                   8.4, "vacances",
                   9.1, "travail",
                   9.2, "travail",
                   9.3, "travail",
                   9.4, "travail",
                   9.5, "travail",
                   9999, NA_character_)

# le code 5.10 est réduit à 5.1 en numeric. 
modes <- tribble(~ code, ~ transport,
                 1.1, "Uniquement marche à pied",
                 1.2, "Porté, transporté en poussette",
                 1.3, "Rollers, trottinette",
                 1.4, "Fauteuil roulant (y compris motorisé)",
                 2.1, "Bicyclette, tricycle (y compris à assistance électrique) sauf vélo en libre service",
                 2.2, "Vélo en libre service",
                 2.3, "Cyclomoteur (2 roues de moins de 50 cm3) – Conducteur",
                 2.4, "Cyclomoteur (2 roues de moins de 50 cm3) – Passager",
                 2.5, "Moto (plus de 50 cm3) – Conducteur (y compris avec side,car et scooter à trois roues)",
                 2.6, "Moto (plus de 50 cm3) – Passager (y compris avec side,car et scooter à trois roues)",
                 2.7, "Motocycles sans précision (y compris quads)",
                 3.1, "Voiture, VUL, voiturette… – Conducteur",
                 3.2, "Voiture, VUL, voiturette… – Passager",
                 3.3, "Voiture, VUL, voiturette… – Tantôt conducteur tantôt passager",
                 3.4, "Trois ou quatre roues sans précision",
                 4.1, "Taxi (individuel, collectif), VTC",
                 4.2, "Transport spécialisé (handicapé)",
                 4.3, "Ramassage organisé par l'employeur",
                 4.4, "Ramassage scolaire",
                 5.1, "Autobus urbain, trolleybus",
                 5.2, "Navette fluviale",
                 5.3, "Autocar de ligne (sauf SNCF)",
                 5.4, "Autre autocar (affrètement, service spécialisé)",
                 5.5, "Autocar TER",
                 5.6, "Tramway",
                 5.7, "Métro, VAL, funiculaire",
                 5.8, "RER, SNCF banlieue",
                 5.9, "TER",
                 #5.10, "Autres transports urbains et régionaux (sans précision)",
                 6.1, "Train à grande vitesse, 1ère classe (TGV, Eurostar, etc)",
                 6.2, "Train à grande vitesse, 2ème classe (TGV, Eurostar, etc)",
                 6.3, "Autre train, 1ère classe",
                 6.4, "Autre train, 2ème classe",
                 6.5, "Train, sans précision",
                 7.1, "Avion, classe première ou affaires",
                 7.2, "Avion, classe premium économique",
                 7.3, "Avion, classe économique",
                 8.1, "Bateau",
                 9.1, "Autre",
                 9999, NA_character_)

AireAttraction <- tribble(~ code, ~ TAA,
                          0L, "Hors zone attraction",
                          1L, "Moins de 50 000 hab.",
                          2L, "De 50 à 200 mille hab.",
                          3L, "De 200 à 700 mille hab.",
                          4L, "Plus de 700 mille hab. hors Paris",
                          5L, "Aire parisienne")

CatCommuneAA <- tribble(~ code, ~ categorie,
                        11, "Commune-centre",
                        12, "Autre commune du pôle principal",
                        13, "Commune pôle secondaire",
                        20, "Commune de la couronne",
                        30, "Commune hors attraction")

CSP <- tribble(~ code, ~CS08, ~ CS24,
               00,  NA_character_, NA_character_,
               10, "Agriculteurs exploitants", "Agriculteurs exploitants",
               21, "Artisans, commerçants et chefs d'entreprise", "Artisans",
               22, "Artisans, commerçants et chefs d'entreprise", "Commerçants et assimilés",
               23, "Artisans, commerçants et chefs d'entreprise", "Chefs d'entreprise de 10 salariés ou plus",
               31, "Cadres et professions intellectuelles supérieures", "Professions libérales et assimilés",
               32, "Cadres et professions intellectuelles supérieures", "Cadres de la fonction publique, professions intellectuelles et artistiques",
               36, "Cadres et professions intellectuelles supérieures", "Cadres d'entreprise",
               41, "Professions Intermédiaires", "Professions intermédiaires de l'enseignement, de la santé, de la fonction publique et assimilés",
               46, "Professions Intermédiaires", "Professions intermédiaires administratives et commerciales des entreprises",
               47, "Ouvriers", "Techniciens",
               48, "Professions Intermédiaires", "Contremaîtres, agents de maîtrise",
               51, "Employés", "Employés de la fonction publique",
               54, "Employés", "Employés administratifs d'entreprise",
               55, "Employés", "Employés de commerce",
               56, "Professions Intermédiaires", "Personnels des services directs aux particuliers",
               61, "Ouvriers", "Ouvriers qualifiés",
               66, "Ouvriers", "Ouvriers non qualifiés",
               69, "Ouvriers", "Ouvriers agricoles",
               71, "Retraités", "Anciens agriculteurs exploitants",
               72, "Retraités", "Anciens artisans, commerçants, chefs d'entreprise",
               73, "Retraités", "Anciens cadres et professions intermédiaires",
               76, "Retraités", "Anciens employés et ouvriers",
               81, "Autres personnes sans activité professionnelle", "Chômeurs n'ayant jamais travaillé",
               82, "Autres personnes sans activité professionnelle", "Inactifs divers (autres que retraités)")

deploc <- deploc |> 
  left_join(motifs, by = c("MMOTIFDES" = "code")) |> 
  left_join(motifs2, by = c("MMOTIFDES" = "code")) |> 
  left_join(modes |> rename("transport1" = "transport"), by = c("MMOY1S" = "code")) |> 
  left_join(modes |> rename("transport2" = "transport"), by = c("MMOY2S" = "code")) |> 
  left_join(modes |> rename("transport3" = "transport"), by = c("MMOY3S" = "code")) |> 
  left_join(modes |> rename("transport4" = "transport"), by = c("MMOY4S" = "code")) |> 
  left_join(AireAttraction |> rename("TAA_ORI" = "TAA"), by = c("TAA2017_ORI" = "code")) |>
  left_join(AireAttraction |> rename("TAA_DES" = "TAA"), by = c("TAA2017_DES" = "code")) |>
  left_join(CatCommuneAA |> rename("CATCOM_AIRE_ORI" = "categorie"), by = c("CATCOM_AA_ORI" = "code")) |> 
  left_join(CatCommuneAA |> rename("CATCOM_AIRE_DES" = "categorie"), by = c("CATCOM_AA_DES" = "code")) |>
  mutate(CS24 = as.integer(CS24)) |> 
  left_join(CSP, by = c("CS24" = "code")) |> 
  rename(CS24_lab = CS24.y)

deploc <- deploc |> 
  mutate(across(.cols = starts_with("TAA_"), .fns = ~ ordered(.x, levels = AireAttraction$TAA))) |> 
  mutate(across(.cols = starts_with("CATCOM_AIRE"), .fns = ~ ordered(.x, levels = CatCommuneAA$categorie)))

breaks_transport <- c(0, 1.9, 2.25, 4.25, 6.6, 7.4, 10000)
labels_transport <- c("walk", "bike", "car", "transit", "plane", "other")

deploc <- deploc |> 
  mutate(TYPMEN5 = factor(TYPMEN5, labels = c("Personne seule", "Monoparent", "Couple sans enfant", "Couple avec enfant(s)", "Autres")))

deploc <- deploc |> 
  mutate(dist_tc = case_when(
    BLOGDIST == 1 ~ "Moins de 300m",
    BLOGDIST == 2 ~ "301 à 600m",
    BLOGDIST == 3 ~ "601 à 999m",
    BLOGDIST == 4 ~ "Plus de 1km",
    TRUE ~ NA_character_
  ))
# construction des modes1 2 3 4 = partie du trajet.
deploc <- deploc |> 
  mutate(across(.cols = MMOY1S:MMOY4S, .fns = \(x) cut(x, breaks = breaks_transport, labels = labels_transport),
                .names = ".temp_{.col}")) |> 
  rename_with(.fn = ~ str_c("mode", str_sub(.x, -2, -2)), .cols = starts_with(".temp_")) 

# attribution de "other" au 2 NA pour mode1 qui ont un IDENT_DEP
deploc <- deploc |> mutate(mode1 = if_else(!is.na(IDENT_DEP) & is.na(mode1), "other", mode1) |> factor(levels = labels_transport)) 

transports <- deploc |> 
  select(mode1:mode4) |> 
  mutate(across(.cols = everything(), .fns = \(x) as.integer(x))) |> 
  as.matrix() |> 
  apply(MARGIN = 1:2, FUN = \(x) ifelse(is.na(x), 0, x)) |> 
  apply(MARGIN = 1:2, FUN = \(x) ifelse(x == 6, 1, x))

nb_modes_wo_walk <- apply(transports, MARGIN = 1, FUN = \(x) keep(x, .p = \(z) z > 1) |> n_distinct())
mode_dominant <- apply(transports, MARGIN = 1, FUN = \(x) max(x, 1))

deploc <- deploc |> 
  mutate(mode = labels_transport[mode_dominant] |> factor(levels = labels_transport),
         unimodal = nb_modes_wo_walk <= 1,
         IDENT_IND = as.character(IDENT_IND),
         IDENT_MEN = as.character(IDENT_MEN),
         IDENT_DEP = as.character(IDENT_DEP),
         motif = factor(motif),
         motif1 = factor(motif1))

# mise à zéro de POND_JOUR si aucun trajet
deploc <- deploc |> mutate(POND_JOUR = if_else(is.na(POND_JOUR), 0, POND_JOUR))

rm(k_individu, modes, motifs, motifs2, q_men, q_voitvul, tcm_ind_kish, tcm_men,
   AireAttraction, CatCommuneAA, CSP, breaks_transport)

calc_boucles <- function(motif) {
  if(all(is.na(motif))) return(0L)
  retour = if_else(motif == "retour", TRUE, FALSE, missing = FALSE) # On pose NA n'est pas un retour
  if (!any(retour)) return(rep(1L, length(motif))) # aucun retour vaut une seule boucle
  if (length(retour) == 1 && motif=="retour") return(0L)
  if (length(retour) == 1) return(1L)
  double = retour & lead(retour, default = FALSE)
  retour = ifelse(double, FALSE, retour)
  
  boucle = 1L + cumsum(as.integer(retour)) - as.integer(retour)
  
  if (first(retour)) {
    if (!last(retour)) { boucle[boucle == max(boucle)] <- 1L } else { 
      boucle[boucle==1] <- NA 
      boucle <- boucle - 1L
    }
  }
  return(boucle)
} 

# Une hiérarchie des motifs :
# travail > etude + accompagner > courses > autres
deploc <- deploc |>
  # drop_na(motif1) |> 
  mutate(motif_principal = fct_collapse(motif1, etudes = c("etudes", "accompagner"), 
                                        autres = c("visite", "demarches", "soins", "loisirs", "vacances")) |> 
           ordered(levels = c("aucun", "autres", "courses", "etudes", "travail"))) |> 
  group_by(IDENT_IND) |> 
  arrange(num_dep, .by_group = TRUE) |> 
  mutate(boucle = calc_boucles(motif1),
         boucle = replace_na(boucle, 0)) |> 
  # drop_na(boucle) |> 
  group_by(IDENT_IND, boucle) |> 
  mutate(nomotif = all(is.na(motif_principal))) |> 
  ungroup() |> 
  # filter(nomotif == FALSE) |> 
  group_by(IDENT_IND, boucle) |> 
  mutate(
    motif_principal = replace_na(motif_principal, "aucun"),
    motif_principal = max(motif_principal),
    n_trajinboucle = n()) |> 
  ungroup()

# on estime le revenu par uc avec les déciles de revenu par uc

revestime <- approxfun(x=1:10, y=c(8358,12982, 15611, 17931, 20121, 22395, 25035, 28505, 34083, 57857))

deploc <- deploc |> 
  mutate(revuce = revestime(decile_rev_uc))


les_descripteurs <- c("IDENT_MEN", "MDATE_jour", "MDATE_mois", "TYPEJOUR", "VAC_SCOL", "NUTS_ORI", "REG_ORI",
                      "SEXE", "AGE", "CS_ACT", "ACTOCCUP", "CS24", "TYPMEN5",   "decile_rev_uc", "quartile_rev_uc", "revuce",
                      "TUU2017_RES", "STATUTCOM_UU_RES", "TYPE_UU_RES", "TAA2017_RES", 
                      "CATCOM_AA_RES", "DENSITECOM_RES", "nb_voitures", "dist_tc",
                      "POND_JOUR", "pond_indC")

# On traite la question des NUTS d'origine qui peuvent être différents
# en trichant un peu
# on pourrait aussi les enlever

# Une base individu avec des list-colonnes informant pour chaque motif des boucles effectuées
deploc_individu <- deploc |> 
  group_by(IDENT_IND) |> 
  mutate(NUTS_ORI = first(NUTS_ORI), 
         REG_ORI = first(REG_ORI)) |> 
  ungroup() |> 
  filter(mobloc == 1|is.na(mobloc)) |> 
  select(IDENT_IND, all_of(les_descripteurs), motif_principal, boucle, distance = MDISTTOT_fin, duree = DUREE, mode) |> 
  group_by(IDENT_IND, motif_principal) |> 
  nest(data = c(boucle, distance, duree, mode)) |> 
  pivot_wider(names_from = motif_principal, values_from = data) |> 
  ungroup()

synthetiser <- function(x) {
  if (is.null(x)) return(NULL)
  group_by(x, mode) |> 
    summarise(distance = sum(distance),
              duree = sum(duree),
              nb_trajets = n(),
              nb_boucles = n_distinct(boucle))}

deploc_individu <- deploc_individu |> 
  mutate(across(.cols = c(travail, etudes, courses, autres, aucun), .fns = \(dat) map(dat, \(x) synthetiser(x)))) |> 
  select(-aucun)

temp_travail <- deploc_individu |> 
  select(IDENT_IND, travail) |> 
  unnest(travail) |> 
  pivot_wider(id_cols = IDENT_IND, names_from = mode, values_from = c(distance, duree, nb_trajets, nb_boucles), values_fill = 0) |> 
  rename_with(.cols = -IDENT_IND, .fn = \(x) paste0(x,"_travail"))

temp_courses <- deploc_individu |> 
  select(IDENT_IND, courses) |> 
  unnest(courses) |> 
  pivot_wider(id_cols = IDENT_IND, names_from = mode, values_from = c(distance, duree, nb_trajets, nb_boucles), values_fill = 0) |> 
  rename_with(.cols = -IDENT_IND, .fn = \(x) paste0(x,"_courses"))

temp_etudes <- deploc_individu |> 
  select(IDENT_IND, etudes) |> 
  unnest(etudes) |> 
  pivot_wider(id_cols = IDENT_IND, names_from = mode, values_from = c(distance, duree, nb_trajets, nb_boucles), values_fill = 0) |> 
  rename_with(.cols = -IDENT_IND, .fn = \(x) paste0(x,"_etudes"))

temp_autres <- deploc_individu |> 
  select(IDENT_IND, autres) |> 
  unnest(autres) |> 
  pivot_wider(id_cols = IDENT_IND, names_from = mode, values_from = c(distance, duree, nb_trajets, nb_boucles), values_fill = 0) |> 
  rename_with(.cols = -IDENT_IND, .fn = \(x) paste0(x,"_autres"))

deploc_individu <- deploc_individu |> 
  left_join(temp_travail, by = "IDENT_IND") |> 
  left_join(temp_courses, by = "IDENT_IND") |> 
  left_join(temp_etudes, by = "IDENT_IND") |> 
  left_join(temp_autres, by = "IDENT_IND")

deploc_individu <- deploc_individu |> 
  select(-c(travail, courses, etudes, autres)) |> 
  mutate(across(.cols = contains(c("travail", "courses", "etudes", "autres")), .fns = \(x) ifelse(is.na(x), 0, x)))

modes_boucle <- deploc |> 
  mutate(IDENT_BCL = str_c(IDENT_IND, "_", boucle)) |> 
  group_by(IDENT_BCL) |> 
  summarize(modes = list(as.character(unique(mode))),
            unimodal = all(unimodal)) |> 
  mutate(modes = map(modes, ~if(length(.x)>1) { setdiff(.x, "walk") } else { .x }),
         mode_bcl = map_chr(modes, ~str_c(sort(.x), collapse="+")),
         mode_bcl = factor(mode_bcl),
         unimodal = if_else(unimodal & !str_detect(mode_bcl, "\\+"), TRUE, FALSE))

deploc_bcl <- deploc |> 
  select(-unimodal) |> 
  mutate(IDENT_BCL = str_c(IDENT_IND, "_", boucle)) |> 
  left_join(modes_boucle, by = "IDENT_BCL")

deploc_bcl <- deploc_bcl |> 
  group_by(IDENT_BCL) |> 
  summarise(
    mobloc = if_else(any(mobloc == 0), 0L, 1L, missing = NA),
    unimodal = first(unimodal),
    motif_principal = first(motif_principal),
    mode_bcl = first(mode_bcl),
    nb_trajets = n(),
    distance = sum(MDISTTOT_fin),
    duree = sum(DUREE),
    across(.cols = all_of(les_descripteurs), \(x) first(x)), 
    .groups = "drop")

deploc_bcl <- deploc_bcl |> 
  mutate(mode_principal = case_when(
    str_detect(mode_bcl, "plane") ~ "plane",
    str_detect(mode_bcl, "car") ~ "car",
    str_detect(mode_bcl, "transit") ~ "transit",
    str_detect(mode_bcl, "bike") ~ "bike",
    TRUE ~ "walk"
  ))

ofce::bd_write(deploc)
ofce::bd_write(deploc_individu)
ofce::bd_write(deploc_bcl)

return(list(deploc=deploc, individu = deploc_individu, bcl = deploc_bcl))

