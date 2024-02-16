import pandas as pd
import numpy as np
from scipy.stats import mstats
import matplotlib.pyplot as plt
from utilities import logoneplus, add_anchors, simplify_dice_metro

def exclude_sic(df):
    df['exclude_sic'] = np.where(
        (df['sic'] < 1000) | ((df['sic'] > 6000) & (df['sic'] < 6500)) | (df['sic'] > 9000),
        1,
        0
    )
    return df

def clean_jolts(jolts_all, statelist):
    jolts_clean = jolts_all.copy()
    jolts_clean.columns = jolts_clean.columns.str.strip()
    jolts_clean['source'] = jolts_clean['series_id'].str[:2]
    jolts_clean['seasadj'] = jolts_clean['series_id'].str[2:3] == "S"
    jolts_clean['sector'] = jolts_clean['series_id'].str[3:9]
    jolts_clean['state'] = jolts_clean['series_id'].str[9:11]
    jolts_clean['size_class'] = jolts_clean['series_id'].str[16:18]
    jolts_clean['data_element'] = jolts_clean['series_id'].str[18:20]
    jolts_clean['LevelOrRate'] = jolts_clean['series_id'].str[20:21]
    jolts_clean = jolts_clean.merge(statelist, left_on='state', right_on='jolts_no', how='left')
    return jolts_clean

def edit(jolts_msadata):
    jolts_msadata_edited = jolts_msadata.copy()
    jolts_msadata_edited = jolts_msadata_edited.dropna()
    jolts_msadata_edited['year'] = (jolts_msadata_edited['period'] // 100).astype(int)
    jolts_msadata_edited['month'] = (jolts_msadata_edited['period'] % 100).astype(int)
    jolts_msadata_edited = jolts_msadata_edited.rename(columns={'msa': 'dice_metro'})
    jolts_msadata_edited = simplify_dice_metro(jolts_msadata_edited)

    jolts_msadata_edited = (jolts_msadata_edited
                        .groupby(['dice_metro', 'year'])
                        .agg({col: 'sum' for col in jolts_msadata_edited.columns if col.endswith('L')}).reset_index())
                            
    return jolts_msadata_edited

def add_jolts_tightness(msa_emp, jolts_msadata_edited):
    msa_emp = msa_emp.rename(columns={"MSA": "dice_metro", "Value": "unemp_count", 'Year': 'year'})
    msa_emp['dice_metro'] = msa_emp['dice_metro'].astype(str)
    msa_emp = simplify_dice_metro(msa_emp)mp['year'] = msa_emp['year'].astype(str)
    jolts_msadata_edited['year'] = jolts_msadata_edited['year'].astype(str)
    msa_emp = msa_emp.dropna()
    msa_emp = msa_emp.merge(jolts_msadata_edited, on=['dice_metro', 'year'], how='inner')
#msa_emp = msa_emp.drop(columns=['dice_metro'])
#msa_emp = msa_emp.drop(columns=['year'])
    msa_emp['tightness'] = msa_emp['JO_L'] / msa_emp['unemp_count']
    return msa_emp

jolts_all = pd.read_csv("../data/jolts_all.txt", delimiter="\t")

statelist = pd.read_csv("../data/statelist.csv")

uswide_unemp = pd.read_csv("../data/uswide_unemp.csv")

jolts_msadata = pd.read_csv("../data/jolts_msadata.csv")

shortjobs_data = pd.read_stata("../data/shortjobs_0dums_multimktfirms_wPMN.dta")

msa_emp = pd.read_csv("../data/msa_unemp_total_yearly.csv")

comp_funda2 = pd.read_csv("../data/comp_funda2.csv")

dhi_crsp = pd.read_stata("../data/dhi_crsp_fullmerge_with_soc.dta")
dhi_crsp['dice_metro'] = dhi_crsp['dice_metro'].str.replace("\\", "")
dhi_crsp = simplify_dice_metro(dhi_crsp)

jolts_clean = clean_jolts(jolts_all, statelist)
jolts_msadata_edited = edit(jolts_msadata)

jolts_msadata_codelist = pd.concat([jolts_msadata_edited['dice_metro'], msa_emp['dice_metro'], dhi_crsp['dice_metro']]).drop_duplicates()

msa_emp = add_jolts_tightness(msa_emp, jolts_msadata_edited)

comp_funda2_edit = comp_funda2[['fyear', 'sic', 'aqc', 'sppe', 'capx']]
comp_funda2_edit = comp_funda2_edit[comp_funda2_edit['fyear'] >= 1971]
comp_funda2_edit = comp_funda2_edit.groupby(['fyear']).apply(exclude_sic)
comp_funda2_edit = comp_funda2_edit[comp_funda2_edit['exclude_sic'] == 0]
comp_funda2_edit['fullsales'] = comp_funda2_edit['aqc'].fillna(0)
comp_funda2_edit['partialsales'] = comp_funda2_edit['sppe'].fillna(0)
comp_funda2_edit['reallocation'] = comp_funda2_edit['partialsales'] + comp_funda2_edit['fullsales']
comp_funda2_edit['capx'] = comp_funda2_edit['capx'].fillna(0)
comp_funda2_edit['expenditures'] = comp_funda2_edit['capx'] + comp_funda2_edit['reallocation']
comp_funda2_edit['pshare'] = comp_funda2_edit['partialsales'] / comp_funda2_edit['reallocation']
comp_funda2_edit['rshare'] = comp_funda2_edit['reallocation'] / comp_funda2_edit['expenditures']
comp_funda2_edit['rshare'] = mstats.winsorize(comp_funda2_edit['rshare'], limits=[0.001, 0.999])
comp_funda2_edit = comp_funda2_edit[['fyear', 'pshare', 'rshare']]
comp_funda2_edit = comp_funda2_edit.dropna()
comp_funda2_edit = comp_funda2_edit.groupby(['fyear']).agg({'pshare': 'mean', 'rshare': 'mean', 'count': 'size'}).reset_index()
comp_funda2_edit['pshare_ma5'] = comp_funda2_edit['pshare'].rolling(window=5, min_periods=1).mean()
comp_funda2_edit['rshare_ma5'] = comp_funda2_edit['rshare'].rolling(window=5, min_periods=1).mean()

plt.plot(comp_funda2_edit['fyear'], comp_funda2_edit['pshare_ma5'])
plt.xlim(1971, 2020)
plt.ylim(0.2, 0.6)
plt.savefig("/Users/pedrovallocci/Documents/PhD (local)/Research/By Topic/Bargaining Power/text/tex/msa_relfreq.png")

dhi_crsp_edit = dhi_crsp.rename(columns={'fyear': 'Year'})
dhi_crsp_edit = dhi_crsp_edit.merge(jolts_msadata_codelist[['msa', 'dice_metro']], left_on='dice_metro', right_on='msa', how='left')
dhi_crsp_edit = dhi_crsp_edit.merge(msa_emp[['msa_code', 'fyear', 'tightness']], left_on=['LPERMNO', 'Year'], right_on=['msa_code', 'fyear'], how='inner')
dhi_crsp_edit = dhi_crsp_edit[['LPERMNO', 'fyear', 'dice_metro', 'pshare', 'rshare', 'tightness']].dropna()
