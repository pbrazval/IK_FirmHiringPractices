library(haven)
library(readr)
library(tidyverse)
setwd("/Users/pedrovallocci/")
dhi_crsp_fullmerge_with_soc <- read_dta("Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_crsp_fullmerge_with_soc.dta")

msa_unemp <- read_csv("Documents/PhD (local)/Research/By Topic/Bargaining Power/data/msa_unemp_counts_yearly.csv")

msa_emp = msa_emp %>%
  rename(dice_metro = MSA) %>%
  rename(fyear = Year) %>%
  rename(unemp = Value)

library(dplyr)

dawg = dhi_crsp_fullmerge_with_soc %>%
  inner_join(msa_emp, by = c('dice_metro', 'fyear'))

db = dhi_crsp_fullmerge_with_soc %>%
  inner_join(msa_emp, by = c('dice_metro', 'fyear')) %>%
  mutate(LPERMNO = as.factor(LPERMNO)) %>%
  mutate(dice_metro = as.factor(dice_metro))

db[is.na(db$short_soc),'short_soc'] <- 9999
db[is.na(db$max_soc),'max_soc'] <- 999999

db = db  %>%
  mutate(max_soc = as.factor(max_soc))
  
no_markets = db %>%
  group_by(LPERMNO) %>%
  summarize(n_markets = n_distinct(dice_metro, max_soc))

logoneplus <- function(x, na.rm = FALSE) log(1+x)
long_db = db %>%
  inner_join(no_markets, by = "LPERMNO") %>%
  mutate_at(c("at", "ppegt", "emp", "sale", "q_tot", "revt", "K_int_Know", "K_int"), logoneplus) %>%
  select(isnumeric_payrate, fyear, n_markets, dice_metro, at, ppegt, emp, ind12, sale, revt, q_tot, K_int_Know, K_int, unemp, LPERMNO, is_senior, is_executive, is_engineer, is_software,short_soc, max_soc)

short_db = long_db %>%
  filter(n_markets > 1)

write_dta(data = short_db, path = "/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/shortjobs_0dums_multimktfirms_wPMN.dta")
write_dta(data = long_db, path = "/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/longjobs_0dums_wPMN.dta")

sdb_named = short_db %>%
  rename(PW = isnumeric_payrate)
library(ggplot2)

pw = ggplot(sdb_named, aes(PW)) + 
  geom_bar(aes(y = (..count..)/sum(..count..)), fill = "blue") + 
  scale_y_continuous(labels=scales::percent) +
  ylab("relative frequencies")
ggsave("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/text/tex/PW_relfreq.png")

by_msa = sdb_named %>% 
  group_by(dice_metro) %>%
  summarize(vacancies = n()/730371) %>%
  ungroup() %>%
  arrange(desc(vacancies)) %>%
  slice(1:10)
ggplot(data=by_msa, aes(x=reorder(dice_metro, vacancies), y=vacancies)) +
  geom_bar(stat = 'identity', fill = "blue")+ 
  ylab("relative frequencies") + xlab("Job type") + coord_flip()
ggsave("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/text/tex/msa_relfreq.png")

by_job = sdb_named %>% 
  group_by(max_soc) %>%
  summarize(share = n()/730371) %>%
  ungroup() %>%
  arrange(desc(share)) %>%
  slice(2:11)

by_job$soc_long = c("Software Developers", "Computer Occupations, All Other", "Computer User Support Specialists", "Web Developers", 
                    "Managers, All Other", "Network and Computer Systems Administrators", "Computer Systems Analysts", "Database Administrators", 
                    "Management Analysts", "Information Security Analysts")

ggplot(data=by_job, aes(x=reorder(soc_long, share), y=share)) +
  geom_bar(stat = 'identity', fill = "blue")+ 
  ylab("relative frequencies") + xlab("Job type") + coord_flip()
ggsave("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/text/tex/jobtype_relfreq.png")

# library(mltools)
# library(data.table)
# library(glmnet)
# library(gamlr)
# 
# model_glm = glm(isnumeric_payrate ~ at + ppegt + emp + K_int_Know + K_int +factor(is_senior) + factor(short_soc) + factor(is_executive) + factor(is_engineer) + factor(is_software) + factor(dice_metro) + factor(LPERMNO), data = short_db, family = binomial("logit"))
# newdata <- one_hot(as.data.table(expl_vars))
# newdata <- newdata[is.finite(rowSums(newdata)),]
# colvec = colnames(indep_vars)
# unpenalized_vars = which(str_detect(colnames(indep_vars), "ind12|dice_metro"))
# model = gamlr(scale(indep_vars), dep_var, free = unpenalized_vars, family = "binomial", maxit = 1e7, gamma = 0 )

# expl_vars = clean_data[,c(-2, -4, -5, -7, -8, -10, -11, -16, -24, -43, -47)]
# rownames(expl_vars) = expl_vars[,1]
# expl_vars = expl_vars[,-1]
# newdata <- one_hot(as.data.table(expl_vars))
#num_data = clean_data[,c(-2, -3, -4, -5, -6, -7, -8, -10, -11, -15, -16, -24, -43, -47)]
# rownames(num_data) = clean_data[,1]
# num_data = num_data[,-1]
# pr.out = prcomp(num_data, scale = TRUE)
# round(pr.out$rotation, 2)
# model = glm((clean_data$Mode) ~ as.numeric(clean_data$Category) + clean_data$COVID_Last60days 
#             + as.numeric(clean_data$per_dem) + (clean_data$HorT) + (clean_data$Hospital)
#             + (clean_data$State))

# model = gamlr(scale(newdata), clean_data$Mode, family = "binomial", gamma = 0 )

  
  