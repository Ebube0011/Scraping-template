from beautifulCrawler import Crawler
from website import Website
import glob
from scraper_settings import WEBSITE_FILE_PATH
import json

sites = []
# get website settings in json files
for jsonfile in glob.glob(WEBSITE_FILE_PATH + "*.json"):
    with open(jsonfile, 'r') as openfile: 
        # Reading from json file 
        json_object = json.load(openfile)
        sites.append(json_object)

if __name__ == '__main__':
    websites = []
    for site in sites:
        website = Website()
        website.name = site['name']
        website.url = site['url']
        website.targetPattern = site['targetTag']
        website.absoluteUrl = site['AbsoluteUrl']
        website.paginationTag = site['nextPageTag']
        website.itemsTag = site['itemsTag']
        website.categoryTag = site['categoryTag']
        website.titleTag = site['titleTag']
        website.ratingTag = site['ratingTag']
        website.priceTag = site['priceTag']
        website.availabilityTag = site['availabilityTag']
        website.linkTag = site['linkTag']

        # add to list of websites
        websites.append(website)

    with Crawler() as crawler:
        for website in websites:
            crawler.crawl(website)
