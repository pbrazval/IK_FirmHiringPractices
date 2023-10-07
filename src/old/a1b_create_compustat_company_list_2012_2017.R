library(haven)
jobs <- read_sas("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/jobs_with_accounts.sas7bdat", 
                               NULL)

library(dplyr)
write.csv(jobs, file = "/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/cleaned_jobs.csv")

selection = jobs %>%
  select(company_name, sic) %>%
  distinct()
  
write.csv(selection, file = "/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_companylist.csv")

library(dplyr)
compustat_companylist = comp_funda2 %>%
  filter(fyear >= 2012) %>%
  filter(fyear <= 2017) %>%
  select(conm, sic, LPERMNO) %>%
  distinct()

write.csv(compustat_companylist, file = "/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/compustat_companylist.csv")
