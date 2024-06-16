from bs4 import BeautifulSoup
import re
import time
import requests
from requests_html import HTMLSession, HTML
import queue
import threading
from scraper_settings import STEPS, STORAGE_TYPE
from scraper_settings import MAX_WORKERS
from scraper_settings import PROXIES, WAIT_TIME, HEADERS
from inspect import getmembers, isfunction
import pipeline_funcs
from content import Content, Dataset
from storage import get_repo, close_pool, get_pool
from fake_useragent import UserAgent
from utils.log_tool import get_logger
from website import Website

logger = get_logger("WEB_SCRAPER")

def get_user_agent():
    '''
    Gets a random user agent
    Returns user agent as string object
    '''
    ua = UserAgent()
    #ua = UserAgent(cache=False, use_cache_server=False)
    user_agent = ua.random
    return user_agent

def check_for_proxies():
    '''
    check if proxies are provided
    '''
    try:
        check = PROXIES['https']
        check = PROXIES['http']
    except KeyError:
        return False
    else:
        return True

# def api_calls(r, *args, **kwargs):
#     calls_left = r.headers['X-Shopify-shop-Api-Call-Limit'].split('/')
#     print(calls_left)
#     calls_made = int(calls_left[0])
#     call_limit = int(calls_left[1])
#     # rate limit api calls to not send too much too quickly
#     if (calls_made == call_limit-2):
#         print('limit close, sleeping')
#         time.sleep(5)

def get_pipeline_funcs():
    '''
    Get all the functions in the pipeline funcs module
    and return them in a dictionary with the name and 
    function address
    '''
    # get pipeline functions
    p_funcs = getmembers(pipeline_funcs, isfunction)

    # add to dictionary
    funcs = {}
    for p_func in p_funcs:
        func_name, func = p_func
        funcs[func_name] = func

    return funcs

#user_agent = get_user_agent()

class BeautifulCrawler:
    def __init__(self):
        # self.session = requests.Session()
        self.session = HTMLSession()
        #self.session.headers.update(headers)
        self.visited = set()
        # self.found = set()
        self.storage = None
        self.pipeline_functions: dict = get_pipeline_funcs()
        self.steps: list[str] = STEPS
        self.proxies_available: bool = check_for_proxies()
        self.work_queue = queue.Queue()
        self.items_scraped:int = 0

        # add storage func to pipeline steps
        if (STORAGE_TYPE == 'db') and (not 'save_to_db' in self.steps):
            logger.debug('Database storage method selected!')
            self.storage = get_repo()
            self.steps.append('save_to_db')
        elif (STORAGE_TYPE == 'obj') and (not 'save_to_obj' in self.steps):
            logger.debug('Object storage method selected!')
            self.storage = get_repo()
            self.steps.append('save_to_obj')
        elif (STORAGE_TYPE == 'excel') and (not 'save_to_excel' in self.steps):
            logger.debug('File storage method selected, data will be stored in .xlsx format')
            self.steps.append('save_to_excel')
        elif (STORAGE_TYPE == 'csv') and (not 'save_to_csv' in self.steps):
            logger.debug('File storage method selected, data will be stored in .csv format')
            self.steps.append('save_to_csv')
        else:
            logger.debug('No valid storage method selected, data will be printed')
            self.steps.append('print_it')
    
    # def on_found_links(self, urls:set[str]):
    #     new = urls - self.found
        
    #     for url in new:
    #         await self.url_queue.put(url)

    def __enter__(self):
        '''
        Things to initailize when using context managers
        '''
        # self.session.hooks['response'] = api_calls

        if (STORAGE_TYPE == 'db'):
            get_pool()
            self.storage.create_tables()
        return self
    
    def get_html_doc(self, doc:str):
        '''
        Takes a html string and converts it to a beautiful soup
        object
        '''
        html = HTML(html=doc)

        return BeautifulSoup(html, 'lxml')
    
    def get_page(self, url):
        '''
        Gets the html of the page and converts it to beautiful soup
        Returns beautiful soup object or None if page was no found
        '''
        # set the headers
        headers = self.get_headers()
        # add proxies
        proxies = self.get_proxies()

        # to update the cookies
        # self.session.cookies.update(other)

        # to see domains
        # cookies.list_domains()
        # cookes.list_paths()

        # clearing cookies
        # self.session.cookies.clear(
        #     # domain=None,
        #     # path=None,
        #     # name=None
        #     )
        # self.session.cookies.copy()
        self.session.cookies.clear_session_cookies()

        # make request
        try:
            time.sleep(WAIT_TIME)
            logger.info(f'Getting page: {url}')
            # response = self.session.get('https://icanhazip.com', proxies=proxies)
            # return response.text.strip()
            response = self.session.get(url,
                                        headers=headers, 
                                        proxies=proxies,
                                        allow_redirects=True,
                                        )
        except requests.exceptions.ConnectionError as e: 
            logger.error('Unable to get page due to lack of network connection!!!')
            return None
        except Exception as e:
            logger.error('Unable to get page!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
            return None
        else:
            if (response.status_code == requests.codes.ok):#(response.status_code == 200):
                # response.html.render(
                #     script=script,
                #     retries= 3,
                #     wait=1.2,
                #     scrolldown=10,
                #     sleep=1,
                #     keep_page=True,
                #     timeout=30,
                # )
                # html = response.text
                html = response.html.html
                return BeautifulSoup(html, 'lxml')
            else:
                if (response.status_code == 400):
                    err_message = "Your request is either wrong or missing some information."
                elif (response.status_code == 401):
                    err_message = "Your request requires some additional permissions."
                elif (response.status_code == 404):
                    err_message = "The request resource doesn't exist."
                elif (response.status_code == 405):
                    err_message = "The endpoint doesn't allow for that specific HTTP method."
                elif (response.status_code == 500):
                    err_message = "Your request wasn't expected and probably broke something on the server side."
                else:
                    err_message = 'Failed request'
                logger.error(f'Response status code- {response.status_code}, reason: {response.reason}')
                logger.error(err_message)
    
    def get_headers(self):
        '''
        Check for provision of headers
        return headers if available, if not return none
        '''
        try:
            check = HEADERS['User-Agent']
        except KeyError:
            return None
        else:
            return HEADERS
    
    def get_proxies(self):
        '''
        Check for provision of proxies
        return proxies if available, if not return none
        '''
        if(self.proxies_available):
            return PROXIES
        else:
            return None

    def clean_attrs(self, attrs):
        '''
        parses attribute arguement into the required format
        Returns a dictionary
        '''
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
        '''
        Parses the tag and string arguments into the right format
        Returns a tuple
        '''
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
        
    def safe_get(self, pageObj, selector:dict):
        '''
        Executes beautiful soup filter functions to get data. In the absence of data
        or presence of an error, it returns an empty string.
        Returns a string
        '''
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
            logger.debug('Tag was not find')
            logger.debug('Moving on!')
            return ''
        except IndexError:
            logger.debug('Issue with select/findall tag, tag not found')
            logger.debug('Moving on!')
            return ''
        return result
                
    def parse_page_data(self, bs:BeautifulSoup, website:Website):
        '''
        Parses the data based on predefined tag definitions. The pipeline function is
        then executed to clean and possibly store the acquired dataset.
        Returns None
        '''

        #logger.info('#' * 25)
        logger.info('Parsing data')
        # parse the data
        category = self.safe_get(bs, website.categoryTag)
        products = self.safe_get(bs, website.itemsTag)

        if (products == '') or (category == ''):
            logger.debug('Category or Products tag missing! Skipping category...')
            return
            
        # instantiate the dataset object
        dataset = Dataset()
        dataset.endpoint = website.name
        
        # loop through all product data
        for product in products:
            # instantiate the content object
            content = Content()
            content.category= category
            content.title= self.safe_get(product, website.titleTag)
            content.rating = self.safe_get(product, website.ratingTag)
            content.price = self.safe_get(product, website.priceTag)
            content.availability = self.safe_get(product, website.availabilityTag)
            content.link = self.safe_get(product, website.linkTag)

            if (website.linkTag['r-url']) and (content.link != ''):
                content.link = '{}/{}'.format(website.url, content.link)
                
            # add product data to the content dataset
            #logger.debug(content)
            dataset.records.append(content)
        
        # send data to pipeline
        self.pipeline(dataset)
                  
            
    def parse(self, url:str, website:Website):
        '''
        Recursive function that isolates next page link and parseable data. 
        Calls the parse function and procedes to the next page, if it exists.
        Returns None
        '''
        # get page
        page = self.get_page(url)
        if page is not None:
            # get book/product links
            self.parse_page_data(page, website)
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
            logger.debug('getting next page tag')
            next_page = self.safe_get(page, website.paginationTag)
            if (next_page != ''):
                # check url for current page
                page_match = re.search(r'(index|page.*).html', url)
                curr_page = page_match.group(0)
                
                # if next page exists, go to the next page
                if (curr_page != next_page):
                    url = url.replace(curr_page, next_page)
                    # get the page data
                    logger.info('going to next page')
                    self.parse(url, website)
        
    def crawl(self, website:Website):
        """
        Get pages from website home page and crawl through filtered links
        Returns None
        """
        # start_time = time.perf_counter()
        bs = self.get_page(website.url)
        if (bs != None):
            # get target pages
            targetPages = self.safe_get(bs, website.targetPattern)

            # loop through pages to get data
            if (targetPages != '') and (len(targetPages) > 0):
                for targetPage in targetPages:
                    #logger.info('-' * 50)
                    # get links of target pages
                    targetPage = targetPage.attrs['href']
                    
                    # add to visited list if not in there
                    if targetPage not in self.visited:
                        self.visited.add(targetPage)
                        if not website.absoluteUrl:
                            targetPage = '{}/{}'.format(website.url, targetPage)
                            
                        # parse page data
                        self.parse(targetPage, website)
                        #logger.info('-' * 50)

    def pipeline(self, dataset:Dataset):
        '''
        Cleans data collected by executing defined pipeline functions
        Returns None
        '''
        # go through pipeline
        #logger.info('>'*25)
        logger.info('beginning pipeline')

        # execute pipeline processes
        if (dataset != None):
            # create dataframe
            if (len(dataset.records) == 0):
                return
            
            # create dataframe
            self.items_scraped += len(dataset.records)
            df = dataset.dataframe()

            # run pipeline steps/stages
            if (self.steps != None) and (len(self.steps) > 0):
                for step in self.steps:
                    for func_name, func in self.pipeline_functions.items():
                        if (step == func_name): #or (step in func_name)
                            logger.debug(f'Executing {func_name} on dataset')
                            df = func(df=df, filename=dataset.endpoint, obj=self)
                            logger.debug(f'Finished execution of {func_name} on dataset')
                        elif (step == 'print_it'):
                            records = df.to_dict(orient='records')
                            for record in records:
                                logger.info(f'Data: {record}')
        
        logger.info('Pipeline process complete')
        #logger.info('>'*25)

    def work(self, name):
        '''
        Consumes a site from the queue to crawl and starts the
        crawling process
        '''
        while not self.work_queue.empty():
            site = self.work_queue.get()
        
            # instantiate the website object
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

            # do the actual task of crawling/scraping
            logger.info(f"Worker-{name} Crawling {website.name}")
            start = time.perf_counter()
            self.crawl(website)
            fin = time.perf_counter() - start
            logger.info(f"Worker-{name} finished crawling {website.name} in time: {fin:.2f}s")

    def delegate_and_run_work(self, sites:list):
        '''
        Creates crawling workers to work concurrently, on threads, to crawl 
        the provided websites
        '''
        # add sites to queue
        for site in sites:
            self.work_queue.put(site)

        # assign workers in different threads
        if(len(sites) < MAX_WORKERS):
            no_workers = len(sites)
        else:
            no_workers = MAX_WORKERS

        workers = [threading.Thread(target=self.work, args=(i,))
                        for i in range(1, no_workers+1)]
        
        logger.info(f"Worker(s) starting work...")
        start = time.perf_counter()

        # start the wokers and wait till they finish
        for worker in workers:
                worker.start()
        for worker in workers:
                worker.join()

        fin = time.perf_counter() - start
        logger.info(f"Total number of pages scraped: {len(self.visited)}")
        logger.info(f"Total number of items scraped: {self.items_scraped}")
        logger.info(f"Worker(s) finished all work in time: {fin:.2f}s")

    def __exit__(self, type, value, traceback):
        '''
        Closes the storage pool on exit from context manager
        '''
        # close the session object
        self.session.close()

        # close the database connection pool if storage type used was database
        if (STORAGE_TYPE == 'db'):
            close_pool()