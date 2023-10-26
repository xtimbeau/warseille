library(tidyverse)
load(here::here("marseille/baselayer.rda"))
# zip::unzip(zipfile = "/scratch/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
#        files = "k_deploc_public.csv")
# zip::unzip(zipfile = "/scratch/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
#            files = "k_individu_public.csv")
# zip::unzip(zipfile = "/scratch//DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
#             files = "tcm_men_public.csv")
# zip::unzip(zipfile = "/scratch/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
#            files = "q_menage_public.csv")
#zip::unzip(zipfile = "/scratch/DVFdata/sources/mobilite/donnees_individuelles_anonymisees_emp2019.zip",
#          files = "q_voitvul_public.csv")
deploc <- read_delim("~/files/k_deploc_public.csv", locale = readr::locale("fr"))
# problems()
k_individu <- read_delim("~/files/k_individu_public.csv", locale = readr::locale("fr"))
# problems()
tcm_men <- read_delim("~/files/tcm_men_public.csv", locale = readr::locale("fr"))
# problems()
q_men <- read_delim("~/files/q_menage_public.csv", locale = readr::locale("fr"))

tcm_ind_kish <- read_delim("~/files/tcm_ind_kish_public.csv", locale = readr::locale("fr"))

tcm_ind_kish <- tcm_ind_kish |> 
  mutate(CS24 = as.integer(CS24))

q_voitvul <- read_delim("~/files/q_voitvul_public.csv", delim = ";",
                        locale = locale("fr", encoding = "ISO-8859-1"))

q_voitvul <- q_voitvul |> 
  select(IDENT_MEN, IDENT_NUMVEH, pond_veh, contains("energie"), KVKMV, KVKM1ANV) |> 
  nest(vehicules = - IDENT_MEN) |> 
  mutate(nb_voitures = map_int(vehicules, nrow))

deploc <- deploc |> 
  left_join(select(k_individu, IDENT_IND, pond_indC, BTCSATISF), by = "IDENT_IND") |> 
  left_join(select(tcm_men, npers = NPERS, nenfants = NENFANTS,
                   ident_men, pond_menC, decile_rev, quartile_rev, TYPMEN5,
                   decile_rev_uc, quartile_rev_uc, contains(c("RES", "res"))), 
            by = c("IDENT_MEN" = "ident_men")) |> 
  left_join(select(q_men, IDENT_MEN, BLOGDIST), by = "IDENT_MEN") |> 
  left_join(select(tcm_ind_kish, ident_ind, AGE, SEXE, ACTOCCUP, CS_ACT, CS24),
            by = c("IDENT_IND" = "ident_ind"))

deploc <- deploc |> 
  left_join(q_voitvul, by = "IDENT_MEN") |> 
  mutate(nb_voitures = replace_na(nb_voitures, 0L))

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
  left_join(CSP, by = c("CS24" = "code"))

deploc <- deploc |> 
  mutate(TYPMEN5 = factor(TYPMEN5, labels = c("Personne seule", "Monoparent", "Couple sans enfant", "Couple avec enfant(s)", "Autres")))

deploc <- deploc |> 
  mutate(across(.cols = starts_with("TAA_"), .fns = ~ ordered(.x, levels = AireAttraction$TAA))) |> 
  mutate(across(.cols = starts_with("CATCOM_AIRE"), .fns = ~ ordered(.x, levels = CatCommuneAA$categorie)))

breaks_transport <- c(0, 1.9, 2.25, 4.9, 6.6)
labels_transport <- c("walk", "bike", "car", "transit")


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

# on se débarrasse des trajets sans mode1 valide
deploc <- deploc |> drop_na(mode1)

multimodaux_mode <- function(a, b, c, d) {
  x <- c(a, b, c, d) |> discard(is.na) |> unique()
  if (length(x) == 1) return(labels_transport[x])
  if (any(x == "car")) return("car")
  if (any(x == "transit")) return("transit")
  return("bike")
}

multimodaux_existence <- function(a, b, c, d) {
  x <- c(a, b, c, d) |> discard(is.na) |> unique()
  length(x) == 1
}

deploc <- deploc |> 
  mutate(mode = pmap_chr(list(mode1, mode2, mode3, mode4), multimodaux_mode),
         unimodal = pmap_lgl(list(mode1, mode2, mode3, mode4), multimodaux_existence),
         IDENT_IND = as.character(IDENT_IND),
         IDENT_MEN = as.character(IDENT_MEN),
         IDENT_DEP = as.character(IDENT_DEP),
         motif = factor(motif),
         motif1 = factor(motif1),
         mode=factor(mode))

rm(k_individu, modes, motifs, motifs2, q_men, q_voitvul, tcm_ind_kish, tcm_men,
   AireAttraction, CatCommuneAA, CSP, breaks_transport)

# les boucles. Si retour en début de journée et non en fin, on "boucle" le début avec la fin.
# parfois deux trajets de suite sont des retours (retour maison secondaire, puis principale par ex.), 
# on les met dans la même boucle.
calc_boucles <- function(motif) {
  retour = as.integer(motif == "retour")
  if (!any(retour == 0)) return(rep(NA, length(motif)))
  if (length(motif) == 1) return(1L)
  double = retour + lead(retour, default = 0L)
  retour = ifelse(double == 2, 0L, retour)
  boucle = 1L + cumsum(retour) - retour
  if (first(retour) == 1) {
    if (last(retour) != 1) { boucle[boucle == max(boucle)] <- 1L } else { boucle[boucle==1] <- NA }
  }
  return(boucle)
} 

# NA négligeable
#deploc |> group_by(motif1) |> summarise(dist = sum(MDISTTOT_fin * POND_JOUR)) 

# Une hiérarchie des motifs.
# travail > etude + accompagner > courses > autres
deploc <- deploc |>
  drop_na(motif1) |> 
  mutate(motif_principal = fct_collapse(motif1, etudes = c("etudes", "accompagner"), 
                                        autres = c("visite", "demarches", "soins", "loisirs", "vacances")) |> 
           ordered(levels = c("autres", "courses", "etudes", "travail"))) |> 
  group_by(IDENT_IND) |> 
  arrange(num_dep, .by_group = TRUE) |> 
  mutate(boucle = calc_boucles(motif1)) |> 
  drop_na(boucle) |> 
  group_by(IDENT_IND, boucle) |> 
  mutate(nomotif = all(is.na(motif_principal))) |> 
  ungroup() |> 
  filter(nomotif == FALSE) |> 
  group_by(IDENT_IND, boucle) |> 
  mutate(motif_principal = max(motif_principal, na.rm = TRUE),
         n_trajinboucle = n()) |> 
  ungroup()

qs::qsave(deploc, here::here("v2/data/depl