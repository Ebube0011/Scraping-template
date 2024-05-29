from bs4 import BeautifulSoup
import re
import time
import requests
from scraper_settings import STEPS, STORAGE_TYPE, STORAGE
from scraper_settings import PROXIES
from scraper_settings import WAIT_TIME
from inspect import getmembers, isfunction
import pipeline_funcs
from content import Content, Dataset
from storage import get_storage, save_to_csv
from fake_useragent import UserAgent

def get_user_agent():
    ua = UserAgent()
    #ua = UserAgent(cache=False, use_cache_server=False)
    user_agent = ua.random
    return user_agent

# get pipeline functions
p_funcs = getmembers(pipeline_funcs, isfunction)

# add to dictionary
funcs = {}
for p_func in p_funcs:
    func_name, func = p_func
    funcs[func_name] = func

#user_agent = get_user_agent()

class Crawler:
    def __init__(self):
        self.site = None
        self.visited = []
        self.storage = get_storage()
        self.pipeline_functions = funcs
        self.steps = STEPS

    def __enter__(self):
        return self
    
    def get_page(self, url):
        try:
            time.sleep(WAIT_TIME)
            print(f'getting page: {url}')
            response = requests.get(url)
        except requests.exceptions.RequestException as e:
            print('ERROR: Unable to get page!!!')
            print(f'Exception: {e.__class__.__name__}: {e}')
            return None
        except Exception as e:
            print(f'Exception: {e.__class__.__name__}: {e}')
        else:
            if (response.status_code == 200):
                return BeautifulSoup(response.text, 'lxml')

    def anon_get_page(self, url):
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
                   'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                   'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8'
                  }
    
        #requests.get('https://icanhazip.com', proxies=proxies).text.strip()
        # check if proxies are available
        proxies_available = None
        try:
            secure = PROXIES['https']
            secure = PROXIES['http']
            proxies_available = True
        except KeyError as e:
            proxies_available = False

        # get the webpage
        try:
            time.sleep(WAIT_TIME)
            print(f'getting page: {url}')
            if(proxies_available):
                response = requests.get(url, 
                                        headers=headers, 
                                        proxies=PROXIES,
                                        #cookies=self.site.cookies
                                    )
            else:
                response = requests.get(url, 
                                        headers=headers,
                                        #cookies=self.site.cookies
                                    )
        except requests.exceptions.RequestException as e:
            print('ERROR: Unable to get page!!!')
            print(f'Exception: {e.__class__.__name__}: {e}')
            return None
        except Exception as e:
            print(f'Exception: {e.__class__.__name__}: {e}')
        else:
            if (response.status_code == 200):
                #self.site.cookies = response.cookies
                return BeautifulSoup(response.text, 'lxml')

    def clean_attrs(self, attrs):
        new_arg = {}
        try:
            for key, value in attrs['attrs'].items():
                if (isinstance(value, list)):
                    if (value[0] == 'pattern'):
                        arg_value = re.compile(value[1])
                    else:
                        arg_value = value[1]
                else:
                    arg_value = value
                new_arg[key] = arg_value
        except KeyError:
            new_arg  = {}

        return new_arg

    def check_args(self, arg):
        # check for the string argument
        try:
            string_arg = arg['string']
        except KeyError:
            string_arg  = ''
        else:
            if (isinstance(string_arg, list)):
                if (string_arg[0] == 'pattern'):
                    string_arg = re.compile(string_arg[1])
                else:
                    string_arg = string_arg[1]
        # check for the name argument
        try:
            name_arg = arg['tag']
        except KeyError:
            name_arg  = ''
        return (name_arg, string_arg)
        
    def safe_get(self, pageObj, selector):
        result = pageObj
        
        # parse pipeline
        try:
            for func, arg in selector.items():
                if func == 'find':
                    tag_attrs = self.clean_attrs(arg)
                    name_arg, string_arg = self.check_args(arg)
                    result = result.find(name=name_arg, string=string_arg, attrs=tag_attrs)
                elif func == 'find_all':
                    tag_attrs = self.clean_attrs(arg)
                    name_arg, string_arg = self.check_args(arg)
                    result = result.find_all(name=name_arg, string=string_arg, attrs=tag_attrs)
                elif func == 'select':
                    result = result.select_one(arg)
                elif func == 'select_all':
                    result = result.select(arg)
                elif func == 'select_text':
                    result = result.select(arg)
                    if result is not None and len(result) > 0:
                        result = '\n'.join([elem.get_text() for
                                            elem in result])
                elif func == 'parent':
                    result = result.parent
                elif func == 'n_sibling':
                    result = result.next_sibling
                elif func == 'index':
                    result = result[arg]
                elif func == 'get':
                    result = result.get(arg)
                elif func == 'text':
                    result = result.get_text().strip()
        except AttributeError:
            # debug message
            print('Tag was not find')
            print('Moving on!')
            return ''
        except IndexError:
            # debug message
            print('Issue with select/findall tag, tag not found')
            print('Moving on!')
            return ''
        return result
                
    def parse(self, bs):

        print('Parsing data')
        # parse the data
        category = self.safe_get(bs, self.site.categoryTag)
        products = self.safe_get(bs, self.site.itemsTag)

        if (products == '') or (category == ''):
            print('Category or Products tag missing! Skipping category...')
            return
            
        # instantiate the dataset object
        dataset = Dataset(endpoint= 'Books')
        
        # loop through all product data
        for product in products:
            # instantiate the content object
            content = Content(category= category,
                              title= self.safe_get(product, self.site.titleTag),
                              rating = self.safe_get(product, self.site.ratingTag),
                              price = self.safe_get(product, self.site.priceTag),
                              availability = self.safe_get(product, self.site.availabilityTag),
                              link = self.safe_get(product, self.site.linkTag)
            )
            if (self.site.linkTag['r-url']) and (content.link != ''):
                content.link = '{}/{}'.format(self.site.url, content.link)
                
            # add product data to the content dataset
            print(content)
            dataset.records.append(content)
        # save data to file
        #save_to_csv(dataset.endpoint, dataset.dataframe())
        #save_to_excel(dataset.endpoint, dataset.dataframe())
        
                
        # send data to pipeline
        self.pipeline(dataset)
                  
            
    def get_page_data(self, url):
        # get page
        page = self.get_page(url)
        if page is not None:
            # get book/product links
            self.parse(page)
            #books = self.safe_get(page, self.bookLinksTag)

            # loop through each individual book
            #for book in books:
                # get book page
                #book_url = self.safe_get(book, self.bookPageTage)
                #book_page = self.get_page(url)
            
                # parse book data
                #if page is not None:
                    #self.parse(book_page)
            
            # get next page url
            print('getting next page tag')
            print('*' * 50)
            next_page = self.safe_get(page, self.site.paginationTag)
            if (next_page != ''):
                # check url for current page
                page_match = re.search(r'(index|page.*).html', url)
                curr_page = page_match.group(0)
                
                # if next page exists, go to the next page
                if (curr_page != next_page):
                    url = url.replace(curr_page, next_page)
                    # get the page data
                    print('going to next page')
                    self.get_page_data(url)
        
    def crawl(self, website):
        """
        Get pages from website home page
        """
        self.site = website
        bs = self.get_page(self.site.url)
        if (bs != None):
            # get target pages
            targetPages = self.safe_get(bs, self.site.targetPattern)

            # loop through pages to get data
            if (targetPages != '') and (len(targetPages) > 0):
                for targetPage in targetPages:
                    print('-' * 100)
                    # get links of target pages
                    targetPage = targetPage.attrs['href']
                    
                    # add to visited list if not in there
                    if targetPage not in self.visited:
                        self.visited.append(targetPage)
                        if not self.site.absoluteUrl:
                            targetPage = '{}/{}'.format(self.site.url, targetPage)
                            
                        # get page data
                        self.get_page_data(targetPage)
                        print('-' * 100)

    def pipeline(self, dataset):
        # go through pipeline
        print('>'*50)
        print('beginning pipeline')

        # execute pipeline processes
        if (dataset != None):
            # create dataframe
            if (len(dataset.records) == 0):
                return
            
            # create dataframe
            df = dataset.dataframe()

            # run pipeline steps/stages
            if (self.steps != None) and (len(self.steps) > 0):
                for step in self.steps:
                    for func_name, func in self.pipeline_functions.items():
                        if (step == func_name): #or (step in func_name)
                            df = func(df)

            # store the data
            if (STORAGE):
                if (STORAGE_TYPE == 'file') and (self.storage == None):
                    print(f'saving data to file: {dataset.endpoint}')
                    save_to_csv(dataset.endpoint, df)
                else:
                    print(f'saving data to {STORAGE_TYPE} storage: {dataset.endpoint}')
                    self.storage.insert_data(dataset.endpoint, df)
            else:
                print(df)
        
        print('Pipeline process complete')
        print('>'*50)
                        
    def __exit__(self, type, value, traceback):
        if (self.storage != None):
            self.storage.close()
