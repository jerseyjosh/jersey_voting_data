from ministers import year_to_cabinet
from data import data_pipeline
import pdb

def is_valid_vote(df):
    df['is_valid_vote'] = df['vote'].isin(['pour', 'contre'])
    return df

def add_vote_stats(vote_counts):
    vote_counts['total_votes'] = vote_counts[['pour','contre']].sum(axis=1)
    vote_counts['pour_pct'] = vote_counts['pour'] / vote_counts['total_votes']
    vote_counts['contre_pct'] = vote_counts['contre'] / vote_counts['total_votes']
    vote_counts['minority_vote'] = vote_counts.apply(lambda x: 'pour' if x['pour'] < x['contre'] else 'contre', axis=1)
    vote_counts['majority_vote'] = vote_counts.apply(lambda x: 'pour' if x['minority_vote']=='contre' else 'contre', axis=1)
    vote_counts['discrepancy'] = vote_counts['pour_pct'] * vote_counts['contre_pct']
    return vote_counts


def is_valid_minister(df, year):
    ministers_list = year_to_cabinet[year]
    df['is_valid_minister'] = df['membername'].isin(ministers_list)
    return df

def votes_to_titles(df):
    vote_counts = df.groupby(['title', 'vote']).size().unstack(fill_value=0)
    return vote_counts

if __name__=="__main__":
    df = data_pipeline('2018-09-01', '2019-11-01
    raw_vote_counts = votes_to_titles(df)
    vote_stats = add_vote_stats(raw_vote_counts)
    
    pdb.set_trace()
