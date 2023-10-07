import pandas as pd, numpy as np, re, ray, time
from fuzzywuzzy import fuzz

redo_merge_dhi_compustat = True
redo_dhi_cleaning = False
redo_interpret_dhi_payrates = True
redo_dict_creation = False

def clean(dirty_column):
    clean_column = []
    for name in dirty_column:
        name = name.lower() #lowercasing
        name = re.sub('\s+', ' ', name)
        name = name.replace(' corp.','')
        name = re.sub(' corp$', '', name)
        name = name.replace(' ltd.','')
        name = name.replace(' ltd','')
        name = name.replace('.com', '')
        name = name.replace(' co.', '')
        name = name.replace('international', 'intl')
        name = name.replace(' llc.','')
        name = name.replace(' llc','')
        name = name.replace(' plc','')
        name = name.replace(' hldgs','')
        name = name.replace(' hldg','')
        name = name.replace('prtnrs','partners')
        name = name.replace('prtrs','partners')
        name = name.replace('info sciences','information sciences')
        name = name.replace('properites','properties')
        name = name.replace('technologes','technologies')
        name = name.replace('properites','properties')
        name = name.replace('solutions','soltns')
        name = name.replace('company','')
        name = name.replace('group','')
        name = name.replace('grp','')
        name = name.replace('gp','')
        name = name.replace('rlty','realty')
        name = name.replace('bank','bk')
        name = name.replace('hq','')
        name = name.replace(' lp','')
        name = name.replace(' llp','')
        name = name.replace(' l.p.','')
        name = name.replace('-adr','')
        #name = name.replace('\\','') #removed1
        name = name.replace(' -lp','')
        name = name.replace(' -cl b','')
        name = name.replace(' -cl a','')
        name = re.sub(r'\binc\b','', name) #twice: with and without periods
        name = re.sub(r'\bcp\b', '', name)
        name = re.sub(r'\bco\b', '', name)
        name = re.sub(r'\bthe\b','',name)
        name = re.sub(r'\bu(\.)?s([\.|a]){0,3}\b','',name)
        name = re.sub('\W','', name)
        clean_column.append(name) #appending cleaned name to new list
    return clean_column

def classify_jobs(full_job_list):
    jobname_column = full_job_list["job_title"]
    senior_col = []
    engineer_col = []
    executive_col = []
    software_col = []
    for name in jobname_column:
        name = str(name)
        name = name.lower()
        is_senior = bool(re.search('senior', name))
        is_engineer = bool(re.search('engineer', name))
        is_executive = bool(re.search('executive', name))
        is_software = bool(re.search('software', name))
        senior_col.append(is_senior) #appending cleaned name to new list
        engineer_col.append(is_engineer) #appending cleaned name to new list
        executive_col.append(is_executive) #appending cleaned name to new list
        software_col.append(is_software) #appending cleaned name to new list
    augmented_table = full_job_list
    augmented_table['is_senior'] = senior_col
    augmented_table['is_engineer'] = engineer_col
    augmented_table['is_executive'] = executive_col
    augmented_table['is_software'] = software_col
    return augmented_table

def jobs_by_firm(dhi_database):
    clean_database = dhi_database.drop(dhi_database[dhi_database.company_country != 'United States'].index)
    summary_by_firm = clean_database.groupby(['company_name', 'dhi_firmcode']).agg(count_rows= ('job_id', 'count'))
    summary_by_firm = clean_database.groupby(['company_name', 'dhi_firmcode']).agg(count_rows= ('job_id', 'count'))
    summary_by_firm.reset_index(inplace=True)
    summary_by_firm.shape
    number_of_duplicates = len(summary_by_firm)-len(summary_by_firm.drop_duplicates('dhi_firmcode'))
    print(str(number_of_duplicates))
    dhi_fullname_vec = summary_by_firm["company_name"]
    dhi_firmcode_vec = summary_by_firm["dhi_firmcode"]
    ratio_of_jobs_vec = summary_by_firm['count_rows']/(np.sum(summary_by_firm['count_rows']))
    return dhi_fullname_vec, dhi_firmcode_vec, ratio_of_jobs_vec

@ray.remote
def decoratedfuzz(idxk):
    this_crsp_alias = crsp_alias_vec[idxk]
    this_crsp_fullname = crsp_fullname_vec[idxk]
    this_permno = permno_vec[idxk]
    best_match = [this_crsp_fullname, np.nan, this_crsp_alias, np.nan, this_permno, np.nan, np.nan, np.nan]
    best_fuzz_score = 0 # Should be here?
    for idx in range(len(dhi_alias_vec)):
        this_dhi_alias = dhi_alias_vec[idx]
        fuzz_score = fuzz.ratio(this_crsp_alias, this_dhi_alias) #calculating the fuzzscore between both company names
        if fuzz_score >= 70 and fuzz_score > best_fuzz_score:
            best_fuzz_score = fuzz_score
            # dhi_sic = dhi_sic_column[idx]
            this_dhi_firmcode = dhi_firmcode_vec[idx]
            this_dhi_fullname = dhi_fullname_vec[idx]
            this_ratio_of_jobs = ratio_of_jobs_vec[idx]
            best_match = np.array([this_crsp_fullname, this_dhi_fullname, this_crsp_alias, this_dhi_alias, this_permno, this_dhi_firmcode, fuzz_score, this_ratio_of_jobs])
    return best_match 

ray.shutdown()
ray.init()
full_job_list = pd.read_csv("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/cleaned_jobs.csv", low_memory=False) 
augmented_job_list = classify_jobs(full_job_list)
augmented_job_list.to_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/output/augmented_jobs.csv') 

if redo_dict_creation:
    full_job_list = pd.read_csv("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/cleaned_jobs.csv") 

    dhi_fullname_vec, dhi_firmcode_vec, ratio_of_jobs_vec = jobs_by_firm(full_job_list)
    dhi_company_df = pd.DataFrame(data = {'dhi_fullname' : dhi_fullname_vec, 'dhi_firmcode_vec' : dhi_firmcode_vec, 'ratio_of_jobs' : ratio_of_jobs_vec})
    dhi_company_df.to_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/output/dhi_company_df.csv') 

    crsp_firm_df = pd.read_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/compustat_companylist.csv')

    dhi_alias_vec = []
    crsp_alias_vec = []
    crsp_fullname_vec = crsp_firm_df["conm"]
    permno_vec = crsp_firm_df["LPERMNO"]

    crsp_alias_vec = clean(crsp_fullname_vec)
    dhi_alias_vec = clean(dhi_fullname_vec)

    dhi_matches = []
    crsp_matches = []
    threshold_values = []

    results = []
    for idxk in range(len(crsp_alias_vec)): 
        print('ratio so far:', idxk/len(crsp_alias_vec)*100) 
        results.append(decoratedfuzz.remote(idxk))
    output = ray.get(results)
    dfA = pd.DataFrame(output)
    # df = df.dropna(subset='DHI name')
    df.columns = ['crsp_fullname', 'dhi_fullname', 'crsp_alias', 'dhi_alias', 'permno', 'dhi_firmcode', 'fuzz_score', 'ratio_of_jobs']

    df.to_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/output/d80pct_20220828.csv') 

    small_dict =  df.sort_values(by = ['dhi_firmcode', 'permno'])
    small_dict = df.drop_duplicates(subset = 'dhi_firmcode')
    small_dict.to_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/output/small_dict_20220828.csv') 

    print('Output printed.')

ray.shutdown()
ray.init()
@ray.remote
def interpret_payrate(idxk):
    thispay = payrate_vec[idxk]
    match = False
    numericpay = np.nan
    if bool(thispay) & isinstance(thispay, str):
        thismatch = re.search('[\d\\,]+[k]?', thispay)
        if bool(thismatch):
            textpay = thismatch.group(0)
            numericmatch = re.search('[\d]+', textpay)
            kmatch = re.search('[k]', textpay)
            if bool(numericmatch):
                thismatch_numericpart = float(numericmatch.group(0))
                if thismatch_numericpart > 0:
                    match = True
                    if bool(kmatch):
                        numericpay = thismatch_numericpart*1000
                    else:
                        numericpay = thismatch_numericpart
    theseresults = [match, numericpay]
    return theseresults 

if redo_merge_dhi_compustat:
    if redo_dhi_cleaning:
        dhi_jobs = pd.read_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/cleaned_jobs.csv')
        permno_dict = pd.read_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/output/small_dict_20220828.csv')
        clean_dhi = dhi_jobs.drop(dhi_jobs[dhi_jobs.company_country != 'United States'].index)
        clean_dhi.drop(['Unnamed: 0', 'Unnamed: 0.1', 'account_id', 'job_id', 'company_city', 'company_state', 'company_country', 'telecommute_option', 'duns_number', 'annual_revenue', 'ownership'], axis = 1, inplace = True)
        permno_dict = permno_dict[permno_dict.fuzz_score >= 98]
        permno_dict = permno_dict[['dhi_firmcode', 'permno']]
        dhi_wpermno = pd.merge(clean_dhi, permno_dict, how = 'inner', on = ['dhi_firmcode', 'dhi_firmcode'])
        dhi_wpermno['fyear'] = pd.to_datetime(dhi_wpermno['first_active_date']).dt.year
        dhi_wpermno = dhi_wpermno.rename(columns={'permno': 'LPERMNO'})
        dhi_wpermno.to_pickle('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_wpermno.pkl')
        del(clean_dhi)
        del(dhi_jobs)
    else:
        dhi_wpermno = pd.read_pickle('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_wpermno.pkl')
    
    if redo_interpret_dhi_payrates:
        payrate_vec = dhi_wpermno['pay_rate']
        results = []
        for idxk in range(len(payrate_vec)): 
            if idxk % 10000 == 0:
                print('ratio so far:', idxk/len(payrate_vec)*100) 
            results.append(interpret_payrate.remote(idxk))
        output = ray.get(results)
        output_df = pd.DataFrame(output)
        output_df.columns = ['isnumeric_payrate', 'payrate_if_numeric']

        dhi_wpermno_wpayrates = pd.concat([dhi_wpermno, output_df], axis=1)
        dhi_wpermno_wpayrates.to_pickle('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_wpermno_wpayrates.pkl')
    else:
         dhi_wpermno_wpayrates = pd.read_pickle('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_wpermno_wpayrates.pkl')
    compclean = pd.read_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/compclean.csv')
    compclean = compclean.rename(columns={'permno': 'LPERMNO'})

    dhi_crsp_fullmerge = pd.merge(dhi_wpermno_wpayrates, compclean, on=['LPERMNO', 'fyear'])
    dhi_crsp_fullmerge.to_pickle('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_crsp_fullmerge.pkl')
else:
    dhi_crsp_fullmerge = pd.read_pickle('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_crsp_fullmerge.pkl')

dhi_crsp_fullmerge.to_csv('/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/data/dhi_crsp_fullmerge.csv')
