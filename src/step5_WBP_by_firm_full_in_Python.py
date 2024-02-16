import pandas as pd
import numpy as np
import os
import warnings
from utilities import logoneplus, add_anchors
import seaborn as sns
warnings.filterwarnings("ignore")
# Path of the current file
current_file_path = __file__

# Directory of the current file
current_dir = os.path.dirname(current_file_path)

# Change the current working directory to the directory of the current file
os.chdir(current_dir)
print("Current working directory: {0}".format(os.getcwd()))

# Read data files
dhi_crsp_fullmerge_with_soc = pd.read_stata("../data/dhi_crsp_with_soc_jan24.dta")
dhi_crsp_fullmerge_with_soc['dice_metro'] = dhi_crsp_fullmerge_with_soc['dice_metro'].str.replace("\\", "")
dhi_crsp_fullmerge_with_soc = add_anchors(dhi_crsp_fullmerge_with_soc)
print("dhi_crsp_fullmerge_with_soc.shape: {0}".format(dhi_crsp_fullmerge_with_soc.shape))

msa_emp = pd.read_csv("../data/msa_unemp_total_yearly.csv")
# Data transformation for msa_emp (assuming it is loaded or created earlier in the script)
msa_emp = msa_emp.rename(columns={"MSA": "dice_metro", "Year": "fyear", "Value": "unemp"})
msa_emp = add_anchors(msa_emp)

# Merging data
db = pd.merge(dhi_crsp_fullmerge_with_soc, msa_emp, how='inner', on=['msa_anchor_city', 'msa_anchor_state', 'fyear'])
db['dice_metro'] = db['msa_anchor_city'] + "," + db['msa_anchor_state']
print("db.shape: {0}".format(db.shape))

# Data transformations
db['LPERMNO'] = db['LPERMNO'].astype('category')
db['dice_metro'] = db['dice_metro'].astype('category')
db['short_soc'] = db['max_soc'] // 100
db.loc[db['short_soc'].isna(), 'short_soc'] = 9999
db.loc[db['max_soc'].isna(), 'max_soc'] = 999999
db['max_soc'] = db['max_soc'].astype('category')

# Creating no_markets dataframe
no_markets = db.groupby('LPERMNO').agg(n_markets=('dice_metro', 'nunique')).reset_index()
print("no_markets.shape: {0}".format(no_markets.shape))
# Function for logarithmic transformation


# Creating long_db dataframe
long_db = pd.merge(db, no_markets, on='LPERMNO')
for column in ['at', 'ppegt', 'emp', 'sale', 'q_tot', 'revt', 'K_int_Know', 'K_int']:
    long_db[column] = long_db[column].apply(logoneplus)

# Selecting and renaming columns
long_db = long_db[['isnumeric_payrate', 'fyear', 'n_markets', 'dice_metro', 'at', 'ppegt', 'emp', 'ind12', 'sale', 'revt', 'q_tot', 'K_int_Know', 'K_int', 'unemp', 'LPERMNO', 'is_senior', 'is_mid', 'is_entry', 'is_none', 'short_soc', 'max_soc']]

# Creating short_db dataframe
short_db = long_db[long_db['n_markets'] > 1]

# Saving data to .dta format (requires 'pandas' version >= 1.0.0)
short_db.to_stata("../data/shortjobs_0dums_multimktfirms_wPMN.dta")
long_db.to_stata("../data/longjobs_0dums_wPMN.dta")

# Sample 1000 lines from short_db
short_db_sample = short_db.sample(n=1000)

# Save short_db sample as CSV
short_db_sample.to_csv("../data/short_db_sample.csv", index=False)

import matplotlib.pyplot as plt

# Creating the plot for short_db
plt.figure(figsize=(10, 6))
sns.histplot(short_db['dice_metro'], color='blue')
plt.xlabel('dice_metro')
plt.ylabel('Count')
plt.title('Histogram of dice_metro')
plt.xticks(rotation=90)

# Limiting the histogram bars to the 10 largest counts
short_db_counts = short_db['dice_metro'].value_counts().nlargest(10)
plt.bar(short_db_counts.index, short_db_counts.values)

# Saving the histogram as an image
plt.savefig("../data/short_db_histogram.png")
plt.close()

# Sample 1000 lines from long_db
long_db_sample = long_db.sample(n=1000)

# Save long_db sample as CSV
long_db_sample.to_csv("../data/long_db_sample.csv", index=False)

print("Finished running step4_WBP_by_firm_full_in_Python.py")
# Renaming columns for visualization
# sdb_named = short_db.rename(columns={"isnumeric_payrate": "PW"})

# # Visualization using Matplotlib (as a replacement for ggplot2 in Python)
# import matplotlib.pyplot as plt
# import seaborn as sns

# # Creating the plot
# plt.figure(figsize=(10, 6))
# sns.histplot(sdb_named['PW'], bins=30, color='blue', stat='probability')
# plt.ylabel('Relative Frequencies')
# plt.show()
