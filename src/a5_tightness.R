jolts_all <- read.delim("~/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/jolts_all.txt")
library(stringr)

statelist <- read.csv("~/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/statelist.csv")

uswide_unemp <- read_csv("Documents/PhD (local)/Research/By Topic/Bargaining Power/data/uswide_unemp.csv")

jolts_msadata <- read_csv("Documents/PhD (local)/Research/By Topic/Bargaining Power/data/jolts_msadata.csv")

read_dta("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/shortjobs_0dums_multimktfirms_wPMN.dta")

load("~/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/comp_funda2.Rdata")


jolts_clean <- jolts_all %>%
  select(-footnote_codes) %>% 
  mutate(source = substr(series_id, 1, 2)) %>%
  mutate(seasadj = substr(series_id,3,3) == "S") %>%
  mutate(sector = substr(series_id, 4, 9)) %>%
  mutate(state = substr(series_id, 10, 11)) %>% #mutate(MSA = substr(series_id, 12, 16)) 
  mutate(size_class = substr(series_id, 17, 18)) %>%
  mutate(data_element = substr(series_id, 19, 20)) %>%
  mutate(LevelOrRate = substr(series_id, 21, 21)) %>%
  left_join(statelist, by = c("state" = "jolts_no"))

# Finding a measure of yearly tightness by state
tightness_bystate = jolts_clean %>%
  filter(LevelOrRate == "R",
         sector == "000000",
         data_element == "JO", 
         seasadj == TRUE) %>%
  select(year, period, value, abbrev) %>%
  group_by(year, abbrev) %>%
  summarize(openings = sum(value))

jolts_msadata_edited = jolts_msadata %>%
  mutate(year = substr(period, 1, 4)) %>%
  mutate(month = substr(period, 5, 6)) %>%
  group_by(msa_code, msa, year) %>%
  summarize(across(ends_with("L"), sum))

msa_dict = msa_emp %>% select(msa) %>% distinct()

jolts_msadata_codelist = jolts_msadata %>%
  select(msa, msa_code) %>%
  distinct() %>%
  full_join(msa_emp %>% select(msa) %>% distinct(), by = c("msa")) %>%
  full_join(dhi_crsp %>% select(dice_metro) %>% distinct(), by = c("msa" = "dice_metro")) %>%
  drop_na(msa)
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Atlanta-Sandy Springs-Marietta\\, GA",'msa_code'] = 12060
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Boston-Cambridge-Nashua, MA-NH",'msa_code'] = 71650
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Boston-Cambridge-Quincy\\, MA-NH",'msa_code'] = 71650
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Chicago-Naperville-Joliet\\, IL-IN-WI",'msa_code'] = 16980
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Denver-Aurora-Broomfield\\, CO",'msa_code'] = 19740
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Dallas-Fort Worth-Arlington\\, TX",'msa_code'] = 19100
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Detroit-Warren-Livonia\\, MI",'msa_code'] = 19820
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Los Angeles-Long Beach-Santa Ana\\, CA",'msa_code'] = 31080
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Houston-Sugar Land-Baytown\\, TX",'msa_code'] = 26420
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Miami-Fort Lauderdale-Pompano Beach\\, FL",'msa_code'] = 33100
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Minneapolis-St. Paul-Bloomington\\, MN-WI",'msa_code'] = 33460
jolts_msadata_codelist[jolts_msadata_codelist$msa == "New York-Northern New Jersey-Long Island\\, NY-",'msa_code'] = 35620
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Philadelphia-Camden-Wilmington\\, PA-NJ-DE-MD",'msa_code'] = 37980
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Phoenix-Mesa-Scottsdale\\, AZ",'msa_code'] = 38060
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Riverside-San Bernardino-Ontario\\, CA",'msa_code'] = 40140
jolts_msadata_codelist[jolts_msadata_codelist$msa == "San Diego-Carlsbad-San Marcos\\, CA",'msa_code'] = 41740
jolts_msadata_codelist[jolts_msadata_codelist$msa == "San Francisco-Oakland-Fremont\\, CA",'msa_code'] = 41860
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Seattle-Tacoma-Bellevue\\, WA",'msa_code'] = 42660
jolts_msadata_codelist[jolts_msadata_codelist$msa == "Washington-Arlington-Alexandria\\, DC-VA-MD-WV",'msa_code'] = 47900
jolts_msadata_codelist = jolts_msadata_codelist %>%
  drop_na()

msa_emp <- read_csv("Documents/PhD (local)/Research/By Topic/Bargaining Power/data/msa_unemp_counts_yearly.csv")

msa_emp = msa_emp %>%
  rename(unemp_count = Value) %>%
  mutate(Year = as.character(Year)) %>%
  left_join(jolts_msadata_codelist, by = c("MSA"= "msa")) %>%
  drop_na() %>%
  inner_join(jolts_msadata_edited, by = c("msa_code", "Year" = "year")) %>%
  select(-MSA) %>%
  select(-1) %>%
  mutate(tightness = JO_L/unemp_count)

exclude_sic = function(df){
  df$exclude_sic = case_when(
    df$sic < 1000 ~ 1,
    df$sic > 6000 & df$sic < 6500 ~ 1,
    df$sic > 9000 ~ 1,
    TRUE ~ 0)
  invisible(df)
}

library(zoo)
library(DescTools)

comp_funda2_edit = comp_funda2 %>%
  select(fyear, sic, aqc, sppe, capx) %>%
  filter(fyear >= 1971) %>% 
  group_modify(~exclude_sic(.x))%>%
  filter(!exclude_sic) %>%
  mutate(fullsales = coalesce(aqc,0)) %>%
  mutate(partialsales = coalesce(sppe,0)) %>%
  mutate(reallocation = partialsales+fullsales) %>%
  mutate(capx = coalesce(capx,0)) %>%
  mutate(expenditures = capx + reallocation) %>%
  mutate(pshare = partialsales/reallocation) %>%
  mutate(rshare = reallocation/expenditures) %>% 
  mutate(rshare = Winsorize(rshare, na.rm = TRUE, probs = c(0.001, 0.999))) %>%
  select(fyear,pshare, rshare) %>%
  drop_na() %>%
  group_by(fyear) %>%
  summarize(pshare = mean(pshare), rshare = mean(rshare), count = n()) %>%
  ungroup() %>%
  mutate(pshare_ma5=rollapply(pshare,width=5,mean,fill=NA,partial = FALSE)) %>%
  mutate(rshare_ma5=rollapply(rshare,width=5,mean,fill=NA,partial = FALSE))
ggplot(data=comp_funda2_edit, aes(x=fyear, y=pshare_ma5)) + geom_line() + xlim(1971, 2020) + ylim(0.2,0.6)
ggplot(data=comp_funda2_edit, aes(x=fyear, y=rshare_ma5)) + geom_line() + xlim(1971, 2020) + ylim(0.1, 0.4)
ggsave("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/text/tex/msa_relfreq.png")

dhi_crsp = read_dta("Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_crsp_fullmerge_with_soc.dta")

jolts_msadata_codelist = jolts_msadata_codelist %>%
  full_join(dhi_crsp %>% select(dice_metro), by = c("msa" = "dice_metro"))

dhi_crsp_edit = dhi_crsp %>%
  mutate(fyear = as.character(fyear)) %>%
  left_join(jolts_msadata_codelist, by = c("dice_metro" = "msa")) %>%#mutate(dice_metro = gsub("\\\\,", ",", dice_metro)) %>%
  left_join(msa_emp, by = c("msa_code", "fyear" = "Year")) %>%
  mutate(fyear = as.numeric(fyear)) %>%
  inner_join(comp_funda2_edit, by = c("LPERMNO", "fyear")) %>%
  group_by(LPERMNO, fyear, dice_metro) %>%
  summarize(pshare = mean(pshare), rshare = mean(rshare), tightness = mean(tightness)) %>%
  drop_na()
# MSA = msa_emp$MSA
# leadstate = str_match(MSA, ",\\s*([^\\-]*)(?:\\-|$)")[,2]
# msa_emp$leadstate = leadstate
