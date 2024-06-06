import glob
import json
from beautifulcrawler import BeautifulCrawler
from scraper_settings import WEBSITE_FILE_PATH

def main():
    ''' 
    Instantiate the crawler object to crawl through 
    the provided websites
    '''
    sites = []

    # get website settings in json files
    for jsonfile in glob.glob(WEBSITE_FILE_PATH + "*.json"):
        with open(jsonfile, 'r') as openfile: 
            # Reading from json file 
            json_object = json.load(openfile)
            sites.append(json_object)

    # crawl websites
    with BeautifulCrawler() as spider_man:
        spider_man.delegate_and_run_work(sites)

if __name__ == '__main__':
    main()
