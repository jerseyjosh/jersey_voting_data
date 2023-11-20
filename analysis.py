from ministers import year_to_cabinet
from data import data_pipeline
from argparse import ArgumentParser
import pandas as pd
import pdb

def trim_valid_ministers(df, year):
    ministers_list = year_to_cabinet[year]
    return df[df['membername'].isin(ministers_list)]

def votes_to_titles(df):
    vote_counts = df.groupby(['title', 'vote']).size().unstack(fill_value=0)
    vote_counts['total_votes'] = vote_counts[['pour','contre']].sum(axis=1)
    vote_counts['pour_pct'] = vote_counts['pour'] / vote_counts['total_votes']
    vote_counts['contre_pct'] = vote_counts['contre'] / vote_counts['total_votes']
    vote_counts['minority_vote'] = vote_counts.apply(lambda x: 'pour' if x['pour'] < x['contre'] else 'contre', axis=1)
    vote_counts['majority_vote'] = vote_counts.apply(lambda x: 'pour' if x['minority_vote']=='contre' else 'contre', axis=1)
    vote_counts['majority_pct'] = vote_counts[['pour_pct','contre_pct']].max(axis=1)
    vote_counts['discrepancy'] = vote_counts['pour_pct'] * vote_counts['contre_pct']
    return vote_counts

def get_disagreement_count(df):
    disagreement_count = df[df['vote'] == df['minority_vote']] \
        .groupby('membername') \
        .size() \
        .sort_values(ascending=False)
    return disagreement_count

if __name__=="__main__":

    parser = ArgumentParser()
    parser.add_argument('--start')
    parser.add_argument('--end')
    args = parser.parse_args()

    start = f'{args.start}-09-01'
    end = f'{args.end}-11-01'
    start_year = pd.to_datetime(start).year

    df = data_pipeline(start, end)
    df = trim_valid_ministers(df, 2018)
    vote_counts = votes_to_titles(df)
    df.to_csv('./data/raw_df.csv')
    vote_counts.to_csv(f'./data/vote_counts_{start_year}.csv')

    merged_df = df.merge(vote_counts, on='title')
    sorted_discrepancy = merged_df.sort_values('discrepancy', ascending=False)
    sorted_discrepancy.to_csv(f'./data/sorted_by_disagreement_{start_year}.csv')

    member_disagreement_count = get_disagreement_count(merged_df)
    member_disagreement_count.to_csv(f'./data/member_disagreement_count+{start_year}.csv')
