import pytest
import pandas as pd
from pipeline_funcs import clean_df

dataset = [
    {
        'title' : "Bitch Planet, Vol. 1: Extraordinary Machine (Bitch Planet (Collected Editions))",
        'rating' : 'Two',
        'price' : 'w£37.92',
        'availability' : 'In stock',
        'category' : 'Sequential Art',
        'link' : 'https://books.toscrape.com/../../../bitch-planet-vol-1-extraordinary-machine-bitch-planet-collected-editions_882/index.html',
    },
    {
        'title' : "The Shadow Hero (The Shadow Hero)",
        'rating' : 'One',
        'price' : 'w£33.14',
        'availability' : 'In stock',
        'category' : 'Sequential Art',
        'link' : 'https://books.toscrape.com/../../../the-shadow-hero-the-shadow-hero_860/index.html',
    },
    {
        'title' : "Fables, Vol. 1: Legends in Exile (Fables #1)",
        'rating' : 'Four',
        'price' : 'w£41.62',
        'availability' : 'In stock',
        'category' : 'Sequential Art',
        'link' : 'https://books.toscrape.com/../../../fables-vol-1-legends-in-exile-fables-1_806/index.html',
    },
]


@pytest.fixture
def df():
    df = pd.DataFrame(data=dataset)
    cleaned_df = clean_df(df)
    return cleaned_df
