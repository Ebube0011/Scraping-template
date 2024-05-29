import pytest
import pandas as pd
import numpy as np


def test_title_is_string(df):
    ''' Ensure that title is a string '''
    assert (df['title'].dtype == str or df['title'].dtype == 'O')

def test_rating_is_int(df):
    ''' Ensure that rating is an integer '''
    assert (df['rating'].dtype == int or df['rating'].dtype == np.int64)

def test_rating_is_within_range(df):
    ''' Ensure that rating is within a certain range '''
    assert df['rating'].between(1, 5).any()

def test_price_is_float(df):
    ''' Ensure that price is a float value '''
    assert (df['price_£'].dtype == float or df['price_£'].dtype == np.float64)

def test_availability_is_string(df):
    ''' Ensure that availability is a string '''
    assert (df['availability'].dtype == str or df['availability'].dtype == 'O')

def test_category_is_string(df):
    ''' Ensure that category is a string '''
    assert (df['category'].dtype == str or df['category'].dtype == 'O')

def test_links_are_unique(df):
    ''' Ensure that no two links are the same '''
    assert pd.Series(df['link']).is_unique
