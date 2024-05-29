import pytest
import pandas as pd
import numpy as np
from pipeline_funcs import clean_link, clean_price, clean_rating, clean_title


                
def test_title_is_string(df):
    ''' Ensure that title is a string '''
    df = clean_title(df)
    assert (df['title'].dtype == 'O')

def test_rating_is_int(df):
    ''' Ensure that rating is an integer '''
    df = clean_rating(df)
    assert (df['rating'].dtype == int or df['rating'].dtype == np.int64)

def test_rating_is_within_range(df):
    ''' Ensure that rating is within a certain range '''
    df = clean_rating(df)
    assert df['rating'].between(1, 5).any()

def test_price_is_float(df):
    ''' Ensure that price is a float value '''
    df = clean_price(df)
    assert (df['price_£'].dtype == float or df['price_£'].dtype == np.float64)

def test_links_are_unique(df):
    ''' Ensure that no two links are the same '''
    df = clean_link(df)
    assert pd.Series(df['link']).is_unique

def test_links_are_valid(df):
    ''' Ensure that the link is valid '''
    df = clean_link(df)
    assert ('/..' not in df['link'])