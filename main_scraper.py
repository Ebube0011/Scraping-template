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

def main():
    ''' Instantiate the crawler to crawl through the provided websites '''

    websites = []
    for site in sites:
        website = Website(name= site['name'],
                          url = site['url'],
                          targetPattern = site['targetTag'],
                          absoluteUrl = site['AbsoluteUrl'],
                          paginationTag = site['nextPageTag'],
                          itemsTag = site['itemsTag'],
                          categoryTag = site['categoryTag'],
                          titleTag = site['titleTag'],
                          ratingTag = site['ratingTag'],
                          priceTag = site['priceTag'],
                          availabilityTag = site['availabilityTag'],
                          linkTag = site['linkTag']
        )

        # add to list of websites
        websites.append(website)

    # crawl websites
    with Crawler() as crawler:
        for website in websites:
            crawler.crawl(website)

if __name__ == '__main__':
    main()