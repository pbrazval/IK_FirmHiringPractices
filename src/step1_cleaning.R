library(dplyr)
library(tidyverse)
library(readr)
library(magrittr)
library(zoo)
library(plm)
library(ggplot2)
library(plot.matrix)
library(haven)

standard_cleaning <- function(df) {
  df %<>% 
    filter(!is.na(curcd) & curcd == "USD") %>%
    filter(!is.na(fic) & fic == "USA") %>%
    filter(!indfmt == "FS") %>%
    filter(!sic %in% (4900:4999)) %>%
    filter(!sic %in% (6000:6999)) %>%
    filter(!sic %in% (9000:9999)) %>%
    distinct(LPERMNO, fyear, .keep_all = TRUE) 
  invisible(df)
}

partial_cleaning <- function(df) {
  df %<>% 
    filter(!is.na(curcd) & curcd == "USD") %>%
    filter(!is.na(fic) & fic == "USA") %>%
    filter(!indfmt == "FS") %>%
    distinct(LPERMNO, fyear, .keep_all = TRUE) 
  invisible(df)
}

assign_depr <- function(df){
  df %<>% 
    mutate(naics4d = as.numeric(substr(naics*1000,1,4))) %>%
    mutate(depr = 0.2) %>%
    mutate(rddepr = case_when(naics4d ==3341 ~ 0.363, 
                              naics4d==5112 ~ 0.308,
                              naics4d==3254 ~ 0.112,
                              naics4d==3344 ~ 0.226,    
                              naics4d==3364 ~ 0.339,
                              naics4d==3342 ~ 0.192,
                              naics4d==5415 ~ 0.489,
                              naics4d==3361 ~ 0.733,
                              naics4d==3362 ~ 0.733,
                              naics4d==3363 ~ 0.733,
                              naics4d==3345 ~ 0.329,
                              naics4d==5417 ~ 0.295,
                              TRUE ~ 0.15))
  invisible(df)
}

interpolate_intan <- function(df){
  df %<>%
    mutate(xsga = case_when(
      is.na(xsga) & !is.na(act) ~ 0,
      is.na(xsga) & is.na(act) ~ na.approx(xsga, na.rm = FALSE),
      !is.na(xsga) ~ xsga), .after = xsga) %>%
    mutate(xrd = case_when(
      is.na(xrd) & !is.na(act) ~ 0,
      is.na(xrd) & is.na(act) ~ na.approx(xrd, na.rm = FALSE),
      !is.na(xrd) ~ xrd), .after = xrd) %>%
    mutate(xsga = coalesce(xsga, 0)) %>%
    mutate(xrd = coalesce(xrd, 0))
  invisible(df)
}

zeroif_na_or_nonpos <- function(df, mylist){
  df %<>%
    mutate(across(all_of(mylist), ~ coalesce(.x,0))) %>%
    mutate(across(all_of(mylist), ~ if_else(.x < 0, 0, .x )))
  invisible(df)
}

dropif_na_or_nonpos <- function(df, mylist){
  df %<>%
    filter(across(all_of(mylist), ~ (!is.na(.x)) & .x > 0))
  invisible(df)
}

perpmethod_rdk <- function(df) {
  df %<>% add_column(rdk = NA)
  for (i in seq_along(df$rdk)){
    if (i == 1){
      df$rdk[i] = 0
    } else {
      df$rdk[i] = df$rdk[i-1]*(1-df$rddepr[i]) + df$xrd[i]
    }
  }
  invisible(df)
}

perpmethod_orgk <- function(df) {
  df %<>% add_column(orgk = NA)
  for (i in seq_along(df$orgk)){
    if (i == 1){
      df$orgk[i] = df$xsga[i]/(df$g[i]+df$depr[i])
    } else {
      df$orgk[i] = df$orgk[i-1]*(1-df$depr[i]) + df$xsga[i]*df$frac[i]
    }
  }
  invisible(df)
}

perpmethod_k  <- function(df){
  df %<>% mutate(kpim = NA)
  for (i in seq_along(df$kpim)){
    if (i == 1){
      df$kpim[i] = df$ppegt[i]
    } else {
      df$kpim[i] = df$kpim[i-1] + df$inet[i]
    }
  }
  invisible(df)
}

create_ind12 <- function(df){
  df %<>% add_column(ind12 = NA)
  seq1 = c(seq(0100,0999),seq(2000,2399),seq(2700,2749),seq(2770,2799),seq(3100,3199),seq(3940,3989))
  seq2 = c(seq(2500,2519),seq(2590,2599),seq(3630,3659),seq(3710,3711),seq(3714,3714),seq(3716,3716),seq(3750,3751),seq(3792,3792),seq(3900,3939),seq(3990-3999))
  seq3 = c(seq(2520,2589),seq(2600,2699),seq(2750,2769),seq(3000,3099),seq(3200,3569),seq(3580,3629),seq(3700,3709),seq(3712,3713),seq(3715,3715),seq(3717,3749),seq(3752,3791),seq(3793,3799),seq(3830,3839),seq(3860,3899))
  seq4 = c(seq(1200,1399), seq(2900,2999))
  seq5 = c(seq(2800,2829), seq(2840,2899))
  seq6 = c(seq(3570,3579),seq(3660,3692),seq(3694,3699),seq(3810,3829),seq(7370,7379))
  seq7 = c(seq(4800,4899))
  seq8 = c(seq(4900,4949))
  seq9 = c(seq(5000,5999),seq(7200,7299),seq(7600,7699))
  seq10 = c(seq(2830,2839),seq(3693,3693),seq(3840,3859),seq(8000,8099))
  seq11 = seq(6000,6999)
  
  df$ind12[df$sic %in% seq1] = 1
  df$ind12[df$sic %in% seq2] = 2
  df$ind12[df$sic %in% seq3] = 3
  df$ind12[df$sic %in% seq4] = 4
  df$ind12[df$sic %in% seq5] = 5
  df$ind12[df$sic %in% seq6] = 6
  df$ind12[df$sic %in% seq7] = 7
  df$ind12[df$sic %in% seq8] = 8
  df$ind12[df$sic %in% seq9] = 9
  df$ind12[df$sic %in% seq10] = 10
  df$ind12[df$sic %in% seq11] = 11
  df$ind12[is.na(df$ind12)] = 12
  invisible(df)
}


reload <- TRUE
clean <- TRUE
usequarterly <- FALSE

if (reload){
  load("../data/comp_funda2.Rdata")
  compselect = comp_funda2 %>% select(`conm`,`LPERMNO`, `fyear`, `act`, `at`, `intan`, `sppe`,
                                      `sic`, `naics`, `xsga`, `gdwl`, `ppegt`, `ppent`,
                                      `aqc`,`fic`, `xrd`, `cogs`, `rdip`, `sale`, `indfmt`, `curcd`,`exchg`,
                                      `prcc_f`, `csho`, `dltt`, `dlc`, `capx`, `emp`, `revt`)
  peterstaylor <- read_csv("../data/peterstaylor.csv")
}

link_gvkey <- read_csv("../data/link_permno_gvkey.csv")

peterstaylor %<>%
  mutate(gvkey = as.numeric(gvkey))

link_gvkey %<>%
  select(GVKEY, LPERMNO, fyear) %>%
  rename(gvkey = GVKEY)

compclean = compselect %>%
  as_tibble() %>%
  # FILTERING PART
  filter(fyear >= 1975) %>% 
  left_join(link_gvkey, by = c("fyear", "LPERMNO")) %>% 
  mutate(gvkey = as.numeric(gvkey)) %>% 
  left_join(peterstaylor, by = c("fyear", "gvkey")) %>% 
  group_modify(~partial_cleaning(.x))%>%
  group_modify(~create_ind12(.x)) %>%
  rename(fullsales = aqc) %>%
  rename(partialsales = sppe) %>%
  mutate(reallocation = partialsales+fullsales) %>%
  mutate(expenditures = capx + reallocation) %>%
  mutate(pshare = partialsales/reallocation) %>%
  mutate(rshare = reallocation/expenditures)


write.csv(compclean, '../data/compclean.csv')

jobs <- read_sas("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/jobs_with_accounts.sas7bdat", NULL)

write.csv(jobs, file = "../data/cleaned_jobs.csv")

selection = jobs %>%
  select(company_name, sic) %>%
  distinct()

write.csv(selection, file = "../data/dhi_companylist.csv")

compustat_companylist = comp_funda2 %>%
  filter(fyear >= 2012) %>%
  filter(fyear <= 2017) %>%
  select(conm, sic, LPERMNO) %>%
  distinct()

write.csv(compustat_companylist, file = "../data/compustat_companylist.csv")

