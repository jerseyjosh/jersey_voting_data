import pandas as pd
import matplotlib.pyplot as plt
import pdb

def read_vote_counts(path):
    vote_counts = pd.read_csv(path)
    return vote_counts

if __name__=="__main__":
    vote_counts_2018 = read_vote_counts('./data/vote_counts_2018.csv')
    vote_counts_2022 = read_vote_counts('./data/vote_counts_2022.csv')
    plt.hist(vote_counts_2018['majority_pct'], label='2018', alpha=0.6, density=True)
    plt.hist(vote_counts_2022['majority_pct'], label='2022', alpha=0.6, density=True)
    plt.legend()
    plt.xlabel('Majority Consensus %')
    plt.ylabel('Normaliz')
    plt.show()
    pdb.set_trace()