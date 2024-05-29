import numpy as np
import re

# pipeline function
def clean_df(df1):
    df = df1.copy()
    
    # remove missing data
    df.dropna(axis=0, how = 'all', inplace=True)
    
    # cleaning text
    #pattern = re.compile('\(.*\)')
    #df['title'] = df['title'].apply(lambda x: re.sub(pattern, '', x))
    df['title'] = df['title'].apply(lambda x: x.replace('\"', ''))
    df['rating'] = df['rating'].apply(lambda x: x.replace('One', '1'))
    df['rating'] = df['rating'].apply(lambda x: x.replace('Two', '2'))
    df['rating'] = df['rating'].apply(lambda x: x.replace('Three', '3'))
    df['rating'] = df['rating'].apply(lambda x: x.replace('Four', '4'))
    df['rating'] = df['rating'].apply(lambda x: x.replace('Five', '5'))
    df['price'] = df['price'].apply(lambda x: x[2:])
    df['link'] = df['link'].apply(lambda x: x.replace('/../../../', '/catalogue/'))
    
    # changing dtypes
    df['title'] = df['title'].astype(np.str_)
    df['rating'] = df['rating'].astype(np.int64)
    df['price'] = df['price'].astype(np.float64)
    #df['price'].apply(lambda x: x[2:]).astype(np.str_)

    # renaming columns
    df.rename(columns={'price': 'price_£'}, inplace=True)
    return df
