import requests
import pandas as pd
import numpy as np
from pprint import pprint
import logging
import pdb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# apply processing in step
def data_pipeline(start, end):
    start, end = pd.to_datetime([start, end])
    years = range(start.year, end.year+1)
    df = get_voting_data(years)
    df = set_dtypes(df)
    df = trim_dates(df, start=start, end=end)
    df = fix_strings(df)
    return df

# fix bad data columns
def realign_columns(df):
    df = df.copy()
    non_numeric_ids = df[pd.to_numeric(df['Id'], errors='coerce').isna()]
    numeric_ids = df[~df.index.isin(non_numeric_ids.index)]

    corrected_data = non_numeric_ids.copy()
    corrected_data.loc[:, 'Vote'] = corrected_data.loc[:, 'MemberName']
    corrected_data.loc[:, 'MemberName'] = corrected_data.loc[:, 'MemberPosition']
    corrected_data.loc[:, 'MemberPosition'] = corrected_data.loc[:, 'PropositionTitle']
    corrected_data.loc[:, 'PropositionTitle'] = corrected_data.loc[:, 'Status']
    corrected_data.loc[:, 'Status'] = corrected_data.loc[:, 'Date']
    corrected_data.loc[:, 'Date'] = corrected_data.loc[:, 'Reference']
    corrected_data.loc[:, 'Reference'] = corrected_data.loc[:, 'Title']
    corrected_data.loc[:, 'Title'] = corrected_data.loc[:, 'Id']
    corrected_data.loc[:, 'Id'] = np.nan

    full_data = pd.concat((numeric_ids, corrected_data), ignore_index=True)
    return full_data

def set_dtypes(df):
    logging.info(f"input dtypes: {df.dtypes}")
    df['Id'] = pd.to_numeric(df['Id'], errors='coerce')
    df['Date'] = pd.to_datetime(df['Date'], unit='ms', errors='coerce')
    logging.info(f"output dtypes: {df.dtypes}")
    df.columns = df.columns.str.lower()
    return df

def fix_strings(df):
    object_cols = df.select_dtypes(include='object').columns
    for col in object_cols:
        df[col] = df[col].str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)
    return df

def trim_dates(df, start, end):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    return df[(df['date']>start) & (df['date']<end)]


def get_voting_data(years):
    
    # The URL from which we're fetching data
    url = 'https://statesassembly.gov.je/Feeds/VotingDataJSON'

    # The headers that are necessary to simulate a browser request
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Referer': 'https://opendata.gov.je/',
    }

    results = []
    for year in years:
        response = requests.get(url+f"?Year={year}", headers=headers)
        if response.status_code == 200:
            results.extend(response.json())  # use extend instead of append
        else:
            logging.error(f"Failed to fetch data for year {year}")
    df = pd.DataFrame(results)

    # Assuming 'df' is your DataFrame and 'Date' is the column with the date information
    df['Date'] = df['Date'].str.extract(r'/Date\((\d+)\+0000\)/')

    return df

