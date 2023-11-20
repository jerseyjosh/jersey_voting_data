import data
import pdb
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if __name__=="__main__":

    df = data.get_voting_data([2018, 2019])
    df = data.set_dtypes(df)
    df = data.trim_df(df, start='2018-09-01', end='2019-11-01')
    df = data.fix_strings(df)

    