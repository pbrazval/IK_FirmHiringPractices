####################################################################################################################
## BEGIN: ADDING PACKAGES AND SETTINGS
####################################################################################################################

import pandas as pd, numpy as np, re, ray, time
from fuzzywuzzy import fuzz

import os

# Set the current working directory to the directory of the script
os.chdir(os.path.dirname(os.path.abspath(__file__)))

redo_merge_dhi_compustat = True
redo_dhi_cleaning = True
redo_interpret_dhi_payrates = True
redo_dict_creation = False
redo_dhi_firmcodes = False
redo_classify_jobs = False

####################################################################################################################
## END: ADDING PACKAGES AND SETTINGS
####################################################################################################################

####################################################################################################################
## DEFINING RELEVANT FUNCTIONS FOR THE FILE
####################################################################################################################

# This function 
def clean(dirty_column):
    clean_column = []  # Initialize an empty list to hold cleaned strings

    # Iterate over each name in the input list
    for name in dirty_column:
        name = name.lower()  # Convert the name to lowercase
        
        # Remove excess white spaces
        name = re.sub('\s+', ' ', name)
        
        # Remove common corporate suffixes45
        name = name.replace(' corp.','')
        name = re.sub(' corp$', '', name)
        name = name.replace(' ltd.','')
        name = name.replace(' ltd','')
        name = name.replace(' llc.','')
        name = name.replace(' llc','')
        name = name.replace(' plc','')
        name = name.replace(' hldgs','')
        name = name.replace(' hldg','')
        
        # Replace commonly used abbreviations with their full forms or vice versa
        name = name.replace('international', 'intl')
        name = name.replace('prtnrs','partners')
        name = name.replace('prtrs','partners')
        name = name.replace('info sciences','information sciences')
        name = name.replace('technologes','technologies')
        name = name.replace('solutions','soltns')
        name = name.replace('rlty','realty')
        name = name.replace('bank','bk')
        
        # Remove certain unwanted words or suffixes
        name = name.replace('.com', '')
        name = name.replace(' co.', '')
        name = name.replace('properites','properties')  # Correcting possible typos
        name = name.replace('company','')
        name = name.replace('group','')
        name = name.replace('grp','')
        name = name.replace('gp','')
        name = name.replace('hq','')
        name = name.replace(' lp','')
        name = name.replace(' llp','')
        name = name.replace(' l.p.','')
        name = name.replace('-adr','')
        #name = name.replace('\\','') # This line is commented out
        name = name.replace(' -lp','')
        name = name.replace(' -cl b','')
        name = name.replace(' -cl a','')
        
        # Remove specific common words using regex to ensure it captures the word as a whole (and not as a substring)
        name = re.sub(r'\binc\b','', name)
        name = re.sub(r'\bcp\b', '', name)
        name = re.sub(r'\bco\b', '', name)
        name = re.sub(r'\bthe\b','',name)
        
        # Remove variations of the word "U.S."
        name = re.sub(r'\bu(\.)?s([\.|a]){0,3}\b','',name)
        
        # Remove non-alphanumeric characters
        name = re.sub('\W','', name)
        
        clean_column.append(name)  # Append the cleaned name to the output list

    return clean_column  # Return the cleaned list

import re  # Import the regular expression module

# Define a function named classify_jobs that takes a DataFrame named job_list as input
def classify_jobs(job_list):
    # Extract the "job_title" column from the DataFrame and store it in jobname_column
    jobname_column = job_list["job_title"]
    
    # Initialize empty lists to store keyword presence for each category
    senior_col = []
    engineer_col = []
    executive_col = []
    software_col = []
    
    # Iterate through each job title in jobname_column
    for name in jobname_column:
        # Convert the job title to a string and make it lowercase for consistent handling
        name = str(name)
        name = name.lower()
        
        # Check if the job title contains specific keywords using regular expressions
        is_senior = bool(re.search('senior', name))
        is_engineer = bool(re.search('engineer', name))
        is_executive = bool(re.search('executive', name))
        is_software = bool(re.search('software', name))
        
        # Append the Boolean values to their respective lists
        senior_col.append(is_senior)  # Appending cleaned name to the senior_col list
        engineer_col.append(is_engineer)  # Appending cleaned name to the engineer_col list
        executive_col.append(is_executive)  # Appending cleaned name to the executive_col list
        software_col.append(is_software)  # Appending cleaned name to the software_col list
    
    # Create a copy of the original DataFrame and store it in augmented_table
    augmented_table = job_list
    
    # Add the newly created columns to the augmented_table DataFrame
    augmented_table['is_senior'] = senior_col
    augmented_table['is_engineer'] = engineer_col
    augmented_table['is_executive'] = executive_col
    augmented_table['is_software'] = software_col
    
    # Return the augmented_table DataFrame with the additional columns
    return augmented_table


# Define a function named jobs_by_firm that takes a DataFrame named dhi_database as input
def jobs_by_firm(dhi_database):
    # Filter the database to keep only records where the company_country is 'United States'
    clean_database = dhi_database.drop(dhi_database[dhi_database.company_country != 'United States'].index)
    
    # Group the clean database by 'company_name' and 'dhi_firmcode', and aggregate the count of job_ids
    summary_by_firm = clean_database.groupby(['company_name', 'dhi_firmcode']).agg(count_rows=('job_id', 'count'))
    
    # Reset the index of the summary_by_firm DataFrame
    summary_by_firm.reset_index(inplace=True)
    
    # Calculate the number of duplicate rows based on the 'dhi_firmcode' column and print it
    number_of_duplicates = len(summary_by_firm) - len(summary_by_firm.drop_duplicates('dhi_firmcode'))
    print(str(number_of_duplicates))
    
    # Extract the 'company_name', 'dhi_firmcode', and calculate the ratio of jobs for each firm
    dhi_fullname_vec = summary_by_firm["company_name"]
    dhi_firmcode_vec = summary_by_firm["dhi_firmcode"]
    ratio_of_jobs_vec = summary_by_firm['count_rows'] / (np.sum(summary_by_firm['count_rows']))
    
    # Return three vectors: company names, firm codes, and the ratio of jobs for each firm
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
if redo_dhi_firmcodes:
    print("Redoing DHI firmcodes")
    dhi_public_names = pd.read_csv("../data/dhi_companylist.csv") 
    job_list = pd.read_csv("../data/cleaned_jobs.csv") 

    ## ASSIGN DHI Firm codes
    dhi_unique_firms = list(set(dhi_public_names['company_name'])) 
    firmcode_map = dict(zip(dhi_unique_firms, range(len(dhi_unique_firms))))
    dhi_public_names['dhi_firmcode'] = [firmcode_map[k] for k in dhi_public_names['company_name']]

    if 'dhi_firmcode' not in job_list.columns:
        job_list['dhi_firmcode'] = [firmcode_map[k] for k in job_list['company_name']]
        job_list.to_csv('../data/cleaned_jobs.csv') 
        print('Full job list updated.')

if 'job_list' not in locals():
    job_list = pd.read_csv("../data/cleaned_jobs.csv", low_memory=False) 

if redo_classify_jobs:   
    print("Redoing classify jobs") 
    job_list = classify_jobs(job_list)
    job_list.to_csv('../data/cleaned_jobs.csv') 

if redo_dict_creation:
    #job_list = pd.read_csv("../data/cleaned_jobs.csv") 

    print("Redoing dict creation")
    dhi_fullname_vec, dhi_firmcode_vec, ratio_of_jobs_vec = jobs_by_firm(job_list)
    dhi_company_df = pd.DataFrame(data = {'dhi_fullname' : dhi_fullname_vec, 'dhi_firmcode_vec' : dhi_firmcode_vec, 'ratio_of_jobs' : ratio_of_jobs_vec})
    dhi_company_df.to_csv('../data/dhi_company_df.csv') 

    crsp_firm_df = pd.read_csv('../data/compustat_companylist.csv')

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
    df = pd.DataFrame(output)
    # df = df.dropna(subset='DHI name')
    df.columns = ['crsp_fullname', 'dhi_fullname', 'crsp_alias', 'dhi_alias', 'permno', 'dhi_firmcode', 'fuzz_score', 'ratio_of_jobs']

    df.to_csv('../data/d80pct_20220828.csv') 

    small_dict =  df.sort_values(by = ['dhi_firmcode', 'permno'])
    small_dict = df.drop_duplicates(subset = 'dhi_firmcode')
    small_dict.to_csv('../data/small_dict_20220828.csv') 

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
    print("Redoing merge DHI compustat")
    if redo_dhi_cleaning:
        #job_list = pd.read_csv('../data/cleaned_jobs.csv')
        permno_dict = pd.read_csv('../data/small_dict_20220828.csv')
        clean_dhi = job_list.drop(job_list[job_list.company_country != 'United States'].index)
        clean_dhi.drop(['Unnamed: 0', 'account_id', 'job_id', 'company_city', 'company_state', 'company_country', 'telecommute_option', 'duns_number', 'annual_revenue', 'ownership'], axis = 1, inplace = True)
        permno_dict = permno_dict[permno_dict.fuzz_score >= 98]
        permno_dict = permno_dict[['dhi_firmcode', 'permno']]
        dhi_wpermno = pd.merge(clean_dhi, permno_dict, how = 'inner', on = ['dhi_firmcode', 'dhi_firmcode'])
        dhi_wpermno['fyear'] = pd.to_datetime(dhi_wpermno['first_active_date']).dt.year
        dhi_wpermno = dhi_wpermno.rename(columns={'permno': 'LPERMNO'})
        dhi_wpermno.to_pickle('../data/dhi_wpermno.pkl')
        del(clean_dhi)
        del(job_list)
    else:
        dhi_wpermno = pd.read_pickle('../data/dhi_wpermno.pkl')
    
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
        dhi_wpermno_wpayrates.to_pickle('../data/dhi_wpermno_wpayrates.pkl')
    else:
         dhi_wpermno_wpayrates = pd.read_pickle('../data/dhi_wpermno_wpayrates.pkl')
    compclean = pd.read_csv('../data/compclean.csv')
    compclean = compclean.rename(columns={'permno': 'LPERMNO'})

    dhi_crsp_fullmerge = pd.merge(dhi_wpermno_wpayrates, compclean, on=['LPERMNO', 'fyear'])
    dhi_crsp_fullmerge.to_pickle('../data/dhi_crsp_fullmerge.pkl')
else:
    dhi_crsp_fullmerge = pd.read_pickle('../data/dhi_crsp_fullmerge.pkl')

dhi_crsp_fullmerge.to_csv('../data/dhi_crsp_fullmerge.csv')
