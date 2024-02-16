import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")
# Path of the current file
current_file_path = __file__

# Directory of the current file
current_dir = os.path.dirname(current_file_path)

# Change the current working directory to the directory of the current file
os.chdir(current_dir)
print("Current working directory: {0}".format(os.getcwd()))

data = pd.read_csv('../data/dhi_crsp_fullmerge.csv')
print("data.shape: {0}".format(data.shape))
crosswalk = pd.read_csv('../data/soc_crosswalk.csv')
print("crosswalk.shape: {0}".format(crosswalk.shape))

merged_data = pd.merge(data, crosswalk, left_on='job_title', right_on='dirty_title', how='left')
merged_data.loc[merged_data['max_soc'].isna(), 'max_soc'] = 999999

print("merged_data.shape: {0}".format(merged_data.shape))

merged_data = merged_data.drop('company_postal_code', axis=1)

merged_data.to_csv('../data/merged_data.csv', index=False)
print("Merged_data saved to CSV.")

# Convert merged_data.csv to dhi_crsp_with_soc_jan24.dta
merged_data.to_stata('../data/dhi_crsp_with_soc_jan24.dta')
print("Merged_data converted to dhi_crsp_with_soc_jan24.dta.")

matched_rows = merged_data.dropna(subset=['max_soc'])
unmatched_rows = merged_data[merged_data['max_soc'].isna()]

print("Number of data rows that had a match in crosswalk: {0}".format(matched_rows.shape[0]))
print("Number of rows that hadn't a match in crosswalk: {0}".format(unmatched_rows.shape[0]))


