

def member_abscence(df):
    df = df.copy()
    df['is_abscent'] = df['vote'].isin(['out of the island', 'ill', 'not present for vote', 'excused', 'absent', 'en defaut', 'excused attendance'])
    return df.groupby('membername')['is_abscent'].mean().sort_values(ascending=False)
    
def member_illness(df):
    df = df.copy()
    df['is_ill'] = df['vote'].isin(['ill'])
    return df.groupby('membername')['is_ill'].mean().sort_values(ascending=False)

def member_out_of_island(df):
    df = df.copy()
    df['is_out_of_island'] = df['vote'].isin(['out of the island'])
    return df.groupby('membername')['is_out_of_island'].mean().sort_values(ascending=False)

def member_votes(df):
    df = df.copy()
    r