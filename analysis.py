from ministers import year_to_cabinet
from data import data_pipeline
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
    vote_counts['discrepancy'] = vote_counts['pour_pct'] * vote_counts['contre_pct']
    return vote_counts

def get_disagreement_count(df):
    disagreement_count = df[df['vote'] == df['minority_vote']] \
        .groupby('membername') \
        .size() \
        .sort_values(ascending=False)
    return disagreement_count

if __name__=="__main__":
    df = data_pipeline('2018-09-01', '2019-11-01')
    df = trim_valid_ministers(df, 2018)
    vote_counts = votes_to_titles(df)
    vote_counts.to_csv('./data/vote_counts_debug.csv')

    merged_df = df.merge(vote_counts, on='title')
    sorted_discrepancy = merged_df.sort_values('discrepancy', ascending=False)
    sorted_discrepancy.to_csv('./data/sorted_by_disagreement.csv')

    member_disagreement_count = get_disagreement_count(merged_df)
    member_disagreement_count.to_csv('./data/member_disagreement_count.csv')

    pdb.set_trace()
