from beautiful_crawler import Crawler
from website import Website
import re

sites = [
    {
        'name':'BookScrape',
        'url':'https://books.toscrape.com',
        'targetTag':{'find_all':{'tag':'a', 'attrs':{'href':re.compile('^(catalogue/category/books/)')}}},
        'AbsoluteUrl':False,
        'nextPageTag':{'select':'li.next', 'index':0, 'find':{'tag':'a', 'attrs':{'href':True}}, 'get':'href'},
        'itemsTag':{'select': 'article.product_pod'},
        'categoryTag':{'select': 'h1', 'index':0, 'text': True},
        'titleTag':{'find': {'tag': 'a', 'attrs': {'title': True}}, 'get': 'title'},
        'ratingTag':{'find': {'tag': 'p','attrs': {'class': re.compile('star-rating.(\w{3,5})$')}},
                     'get': 'class','index': 1},
        'priceTag':{'select': 'p.price_color', 'index':0, 'text': True},
        'availabilityTag':{'select': 'p.instock.availability', 'index':0, 'text': True},
        'linkTag':{'find': {'tag': 'a', 'attrs': {'href': True}}, 'get': 'href', 'r-url': True}
    },
]

def lambda_handler(event, context):
    websites = []
    for site in sites:
        websites.append(Website(site['name'], site['url'], site['targetTag'], site['AbsoluteUrl'],site['nextPageTag'], 
                                site['itemsTag'], site['categoryTag'], site['titleTag'], site['ratingTag'], 
                                site['priceTag'], site['availabilityTag'], site['linkTag']))
    for site in sites:
        with Crawler() as crawler:
            for website in websites:
                crawler.crawl(website)

    # can also return dataframe converted to json
    return {'statuscode':200}