import pandas as pd
import requests
import json
import numpy as np
import os
import creds

# Path of the current file
current_file_path = __file__

# Directory of the current file
current_dir = os.path.dirname(current_file_path)

# Change the current working directory to the directory of the current file
os.chdir(current_dir)

def read_msa_data(file_path, ticker):
    df = pd.read_csv(file_path)
    msa_dict = dict(df[[ticker, 'msa']].values)
    msa_list = list(df[ticker])
    return msa_dict, msa_list

def fetch_data(msa_list, start_year, end_year, registration_key):
    headers = {'Content-type': 'application/json'}
    json_data = []
    for i in range(0, len(msa_list), 50):
        sublist = msa_list[i:i+50]
        data = json.dumps({
            "seriesid": sublist,
            "catalog": True,
            "startyear": start_year,
            "endyear": end_year,
            "calculations": True,
            "annualaverage": True,
            "registrationkey": registration_key
        })
        response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        if response.status_code == 200:
            json_data.append(json.loads(response.text))
        else:
            print(f"Error fetching data: {response.status_code}")
    return json_data

def process_data(json_data, msa_dict):
    msa_vector, seriesid_vector, year_vector, period_vector, value_vector = [], [], [], [], []
    for data in json_data:
        for series in data['Results']['series']:
            for item in series['data']:
                msa_vector.append(msa_dict[series['seriesID']])
                seriesid_vector.append(series['seriesID'])
                year_vector.append(item['year'])
                period_vector.append(item['period'])
                value_vector.append(item['value'])
    return pd.DataFrame({'MSA': msa_vector, 'Year': year_vector, 'Period': period_vector, 'Value': value_vector})

def save_csv(output_df, file_path, query=None):
    if query:
        output_df = output_df.query(query).drop(columns='Period')
    output_df.to_csv(file_path)

## Fetching data for unemployment rate
file_path = '../data/msa_emp_tickers_manual_changes.csv'

msa_dict, msa_list = read_msa_data(file_path, 'unemp_rate')

json_data = fetch_data(msa_list, "2010", "2018", creds.api_key)

output_df = process_data(json_data, msa_dict)
save_csv(output_df, '../data/msa_unemp_rates.csv')

save_csv(output_df, '../data/msa_unemp_rates_yearly.csv', query="Period == 'M13'")

## Fetching data for total unemployment
msa_dict, msa_list = read_msa_data(file_path, 'unemp_total')

json_data = fetch_data(msa_list, "2010", "2018", creds.api_key)

output_df = process_data(json_data, msa_dict)
save_csv(output_df, '../data/msa_unemp_total.csv')

save_csv(output_df, '../data/msa_unemp_total_yearly.csv', query="Period == 'M13'")