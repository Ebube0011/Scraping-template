import json
from scraper_settings import WEBSITE_FILE_PATH

'''
Uses the beautiful soup methods as a foundation to dynamically parse the html data.
The methods are coded into dictionaries in order of operation to form a pipeline.
Api response parsing is also included in the form of a list in the order of operations.
'''

def load_to_json(website_config:dict):

    # insert the website tags into a json file
    json_object = json.dumps(website_config, indent = 4) 

    # Writing to sample.json
    filename = WEBSITE_FILE_PATH + website_config['name'] + '.json'
    with open(filename, "w") as outfile: 
        outfile.write(json_object)
    
    # print the output result
    print(json_object)

website = {
    "name": "BookScrape",
    "url": "https://books.toscrape.com",
    "targetTag": {"find_all": {"tag": "a","attrs": {"href": ["pattern","^(catalogue/category/books/)"]}}},
    "AbsoluteUrl": False,
    "paginationTag": {"select": "li.next","find": {"tag": "a","attrs": {"href": True}},"get": "href"},
    "itemsTag": {"select_all": "article.product_pod"},
    "categoryTag": {"select": "h1","text": True},
    "titleTag": {"find": {"tag": "a","attrs": {"title": True}},"get": "title"},
    "ratingTag": {"find": {"tag": "p","attrs": {"class": ["pattern","star-rating.(\\w{3,5})$"]}},"get": "class", "index": 1},
    "priceTag": {"select": "p.price_color","text": True},
    "availabilityTag": {"select": "p.instock.availability","text": True},
    "linkTag": {"find": {"tag": "a","attrs": {"href": True}},"get": "href","r-url": True}
}

if __name__ == '__main__':
    load_to_json(website_config=website)