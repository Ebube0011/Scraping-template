import os
import numpy as np
from numpy import nan
import re
from utils.log_tool import get_logger
from scraper_settings import OUTPUT_FILE_DIRECTORY

logger = get_logger("WEB_SCRAPER")


def remove_nulls(**kwargs):
    '''
    Remove rows that are all null 
    '''
    df = kwargs['df'].copy()

    try:
        # remove missing data
        df.replace(to_replace='', value=nan, inplace=True)
        df.dropna(axis='index', how = 'all', inplace=True)#how: any | all
        df.dropna(axis='index', subset=['title', 'link'], inplace=True) # define in which cols to find the missing values
        # df.dropna(axis='index', thresh=2, inplace=True)# 0=index, 1|columns
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    finally:
        return df

def clean_title(**kwargs):
    '''
    Clean the title and change the data type
    '''
    df = kwargs['df'].copy()
    try:
        # remove any quotations in title
        #df['title'] = df['title'].apply(lambda x: x.replace('\"', ''))
        #df['title'] = df['title'].replace(to_replace=r'["\']', value='', regex=True)
        pattern = re.compile('["\']*')
        df['title'] = df['title'].apply(lambda x: re.sub(pattern, '', x))
        # df['title'] = df['title'].apply(lambda x: x.decode('utf-8'))
        
        # set dtype
        df['title'] = df['title'].astype(np.str_)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    finally:
        return df



def clean_rating(**kwargs):
    '''
    Clean the rating and correct the datatype
    '''
    df = kwargs['df'].copy()
    
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
    finally:
        return df

def clean_price(**kwargs):
    '''
    Clean the price, change datatype and rename column
    '''
    df = kwargs['df'].copy()
    
    try:
        df['price'] = df['price'].apply(lambda x: x[2:])
        #df['price'].replace(to_replace=r'[a-z!$£]*', value='', regex=True, inplace=True)
        
        # changing dtypes
        df['price'] = df['price'].astype(np.float64)

        # renaming columns
        df.rename(columns={'price': 'price_£'}, inplace=True)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    finally:
        return df

def clean_link(**kwargs):
    '''
    Clean the link and set dtype to string
    '''
    df = kwargs['df'].copy()

    try:
    # clean the link
        #df['link'] = df['link'].apply(lambda x: x.replace('/../../../', '/catalogue/'))
        df['link'] = df['link'].replace(to_replace=r'(/..)+/', value='/catalogue/', regex=True)
        
        # changing dtypes
        df['link'] = df['link'].astype(np.str_)
    except Exception as e:
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    finally:
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

def save_to_csv(**kwargs):

    filename = OUTPUT_FILE_DIRECTORY + kwargs['filename']  + '.csv'
    df = kwargs['df'].copy()
    try:
        # append if file exists
        file_exists = os.path.isfile(filename)
        if (file_exists):
            df.to_csv(filename, mode='a', index=False, header=False, encoding='utf-8')
        # if not, create new file
        else:
            df.to_csv(filename, index=False)
    except Exception as e:
        logger.error('Failed to save data to file!!!')
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    else:
        logger.info(filename + ' saved Successfully!')
    finally:
        return df


def save_to_excel(**kwargs):

    filename = OUTPUT_FILE_DIRECTORY + kwargs['filename']  + '.xlsx'
    df = kwargs['df'].copy()
    try:
        # append if file exists
        file_exists = os.path.isfile(filename)
        if (file_exists):
            df.to_excel(filename, mode='a', index=False, header=False)
        # if not, create new file
        else:
            df.to_excel(filename, index=False)
    except Exception as e:
        logger.error('Failed to save data to file!!!')
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    else:
        logger.debug(filename + ' saved Successfully!')
    finally:
        return df
    
def save_to_db(**kwargs):
    '''
    inserts dataframe data into database
    '''

    df = kwargs['df'].copy()
    table_name = kwargs['filename']
    storage = kwargs['obj'].storage
    try:
        logger.info(f'saving data to Database storage, table: {table_name}')
        storage.insert_data(table_name, df)
    except Exception as e:
        logger.error('Failed to save data to database!!!')
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    finally:
        return df