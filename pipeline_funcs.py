import numpy as np
from numpy import nan
import re
from utils.log_tool import get_logger

logger = get_logger("WEB_SCRAPER")


def remove_nulls(df1):
    '''
    Remove rows that are all null 
    '''
    df = df1.copy()

    try:
        # remove missing data
        df.replace(to_replace='', value=nan, inplace=True)
        df.dropna(axis='index', how = 'all', inplace=True)#how: any | all
        df.dropna(axis='index', subset=['title', 'link'], inplace=True) # define in which cols to find the missing values
        # df.dropna(axis='index', thresh=2, inplace=True)# 0=index, 1|columns
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    return df

def clean_title(df1):
    '''
    Clean the title and change the data type
    '''
    df = df1.copy()
    try:
        # remove any quotations in title
        #df['title'] = df['title'].apply(lambda x: x.replace('\"', ''))
        #df['title'] = df['title'].replace(to_replace=r'["\']', value='', regex=True)
        pattern = re.compile('["\']*')
        df['title'] = df['title'].apply(lambda x: re.sub(pattern, '', x))
        
        # set dtype
        df['title'] = df['title'].astype(np.str_)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    return df



def clean_rating(df1):
    '''
    Clean the rating and correct the datatype
    '''
    df = df1.copy()
    
    try:
        df['rating'] = df['rating'].apply(lambda x: x.replace('One', '1'))
        df['rating'] = df['rating'].apply(lambda x: x.replace('Two', '2'))
        df['rating'] = df['rating'].apply(lambda x: x.replace('Three', '3'))
        df['rating'] = df['rating'].apply(lambda x: x.replace('Four', '4'))
        df['rating'] = df['rating'].apply(lambda x: x.replace('Five', '5'))
    
        # changing dtypes
        df['rating'] = df['rating'].astype(np.int64)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    return df

def clean_price(df1):
    '''
    Clean the price, change datatype and rename column
    '''
    df = df1.copy()
    
    try:
        df['price'] = df['price'].apply(lambda x: x[2:])
        #df['price'].replace(to_replace=r'[a-z!$£]*', value='', regex=True, inplace=True)
        
        # changing dtypes
        df['price'] = df['price'].astype(np.float64)

        # renaming columns
        df.rename(columns={'price': 'price_£'}, inplace=True)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    return df

def clean_link(df1):
    '''
    Clean the link and set dtype to string
    '''
    df = df1.copy()

    try:
    # clean the link
        #df['link'] = df['link'].apply(lambda x: x.replace('/../../../', '/catalogue/'))
        df['link'] = df['link'].replace(to_replace=r'(/..)*/', value='/catalogue/', regex=True)
        
        # changing dtypes
        df['link'] = df['link'].astype(np.str_)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    return df


# def clean_title(df1):
#     '''
#     Clean
#     '''
#     df = df1.copy()
    
#     # cleaning text
#     #pattern = re.compile('\(.*\)')
#     #df['title'] = df['title'].apply(lambda x: re.sub(pattern, '', x))
#     df['title'] = df['title'].apply(lambda x: x.replace('\"', ''))
#     df['rating'] = df['rating'].apply(lambda x: x.replace('One', '1'))
#     df['rating'] = df['rating'].apply(lambda x: x.replace('Two', '2'))
#     df['rating'] = df['rating'].apply(lambda x: x.replace('Three', '3'))
#     df['rating'] = df['rating'].apply(lambda x: x.replace('Four', '4'))
#     df['rating'] = df['rating'].apply(lambda x: x.replace('Five', '5'))
#     df['price'] = df['price'].apply(lambda x: x[2:])
#     df['link'] = df['link'].apply(lambda x: x.replace('/../../../', '/catalogue/'))
    
#     # changing dtypes
#     df['title'] = df['title'].astype(np.str_)
#     df['rating'] = df['rating'].astype(np.int64)
#     df['price'] = df['price'].astype(np.float64)
#     #df['price'].apply(lambda x: x[2:]).astype(np.str_)

#     # renaming columns
#     df.rename(columns={'price': 'price_£'}, inplace=True)
#     return df
