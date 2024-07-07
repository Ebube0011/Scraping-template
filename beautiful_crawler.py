from bs4 import BeautifulSoup
import re
import time
import requests
from dataclasses import asdict
from pandas import json_normalize
from requests_html import HTMLSession, HTML
import queue
# import threading
from concurrent.futures import ThreadPoolExecutor
from scraper_settings import STEPS, STORAGE_TYPE
from scraper_settings import MAX_WORKERS, RECORDS_BUFFER
from scraper_settings import PROXIES, WAIT_TIME, HEADERS, MAX_RETRIES
from inspect import getmembers, isfunction
import pipeline_funcs
from content import Content
from storage import get_repo, close_pool, get_pool
#from fake_useragent import UserAgent
from utils.log_tool import get_logger
from website import Website

logger = get_logger("WEB_SCRAPER")

# def get_user_agent():
#     '''
#     Gets a random user agent
#     Returns user agent as string object
#     '''
#     ua = UserAgent()
#     #ua = UserAgent(cache=False, use_cache_server=False)
#     user_agent = ua.random
#     return user_agent

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
    
def get_headers():
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

def get_proxies():
    '''
    Check for provision of proxies
    return proxies if available, if not return none
    '''
    if(check_for_proxies()):
        return PROXIES
    else:
        return None

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
        # self.session = HTMLSession()
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
    
    def get_page(self, 
                 session:HTMLSession,
                 url:str, 
                 headers:dict=get_headers(), 
                 proxies:dict=get_proxies()):
        '''
        Gets the html of the page and converts it to beautiful soup
        Returns beautiful soup object or None if page was no found
        Args: 
        url(str): The page link to enter
        headers(dict): Headers to send with page requests
        proxies(dict): Proxies to use when sending page requests

        Returns:
        BeautifulSoup: page beautiful soup object 
        '''
        # update the cookies
        # self.session.cookies.update(other)

        # to see domains
        # cookies.list_domains()
        # cookes.list_paths()

        # clear cookies
        # self.session.cookies.clear(
        #     # domain=None,
        #     # path=None,
        #     # name=None
        #     )
        # self.session.cookies.copy()
        # self.session.cookies.clear_session_cookies()
        session.cookies.clear_session_cookies()

        # make request
        retries:int = 0
        while (retries <= MAX_RETRIES):
            try:
                logger.info(f'Getting page: {url}')
                time.sleep(WAIT_TIME)
                # response = self.session.get('https://icanhazip.com', proxies=proxies)
                # return response.text.strip()
                # response = self.session.get(url,
                response = session.get(url,
                                            headers=headers, 
                                            proxies=proxies,
                                            allow_redirects=True,
                                            )
                logger.debug(f'Request Response is of type: {response.headers.get("Content-Type")}')
                break
            except requests.exceptions.ConnectionError as e: 
                logger.error('Unable to get page due to lack of network connection!!!')
                logger.debug('Retrying connection')
                retries += 1
                if (retries > MAX_RETRIES):
                    logger.error(f'Unable to get page after maximum number of retries({retries})!!!')
                    return None
            except Exception as e:
                logger.error('Unable to get page!!!')
                logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                retries += 1
                if (retries > MAX_RETRIES):
                    logger.error(f'Unable to get page after maximum number of retries({retries})!!!')
                    return None
        
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
            html = response.text
            # html = response.html.html
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
            return None
    
    def get_api(self, 
                 session:HTMLSession,
                 endpoint:str, 
                 params:dict,
                 headers:dict, 
                 proxies:dict=get_proxies()):
        '''
        Make api requests
        Returns json object or None if the request failed
        Args: 
        endpoint(str): The link endpoint to which the request is being made
        params(dict): Optional parameters to send with the request
        headers(dict): Headers to send with request
        proxies(dict): Proxies to use when sending requests

        Returns:
        json: request response 
        '''
        # clear the cookies
        session.cookies.clear_session_cookies()

        # make request
        retries:int = 0
        while (retries <= MAX_RETRIES):
            try:
                logger.info(f'Making request to endpoint: {endpoint}')
                time.sleep(WAIT_TIME)
                # response = self.session.get('https://icanhazip.com', proxies=proxies)
                # return response.text.strip()
                # response = self.session.get(url,
                response = session.get(endpoint,
                                       params=params,
                                       headers=headers, 
                                       proxies=proxies,
                                       allow_redirects=True,
                                       )
                logger.debug(f'Request Response is of type: {response.headers.get("Content-Type")}')
                break
            except requests.exceptions.ConnectionError as e: 
                logger.error('Unable to make request due to lack of network connection!!!')
                logger.debug('Retrying connection')
                retries += 1
                if (retries > MAX_RETRIES):
                    logger.error(f'Unable to make request after maximum number of retries({retries})!!!')
                    return None
            except Exception as e:
                logger.error('Unable to make request!!!')
                logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                retries += 1
                if (retries > MAX_RETRIES):
                    logger.error(f'Unable to make request after maximum number of retries({retries})!!!')
                    return None
        
        if (response.status_code == requests.codes.ok):#(response.status_code == 200):
            return response.json()
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
            return None

    def clean_attrs(self, attrs):
        '''
        parses attribute arguement into the required format
        Returns:
        dict: tag attributes better arranged
        '''
        new_arg = {}
        try:
            for key, value in attrs['attrs'].items():
                # extract the pattern and convert to regex
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

    def sort_parse_args(self, arg):
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
        
    # def safe_get(self, pageObj:BeautifulSoup=None, api_response:dict=None, selector:dict|list = None):
    #     '''
    #     Executes either a function to parse bs page or api. The selector type
    #     is used to identify which function to call when parsing the data.
    #     Returns the result of the called function
    #     '''
    #     if (isinstance(selector, dict)):
    #         result = self.safe_page_data_get(pageObj, selector)
    #         return result
    #     elif (isinstance(selector, list)):
    #         result = self.safe_api_get(api_response, selector)
    #         return result
    #     else:
    #         logger.error('Invalid tag/api selector used!!!')
    #         return ''

        
    def safe_page_data_get(self, bs_page:BeautifulSoup, selector:dict):
        '''
        Executes beautiful soup filter functions to get data. In the absence of data
        or presence of an error, it returns an empty string.

        Args: 
        bs_page(BeautifulSoup): The bs page or tag-element object to be parsed
        selector(dict): dictionary of tags to be used to parsed the page object

        Returns:
        TagElement | str: A tag element object or string data
        '''
        result = bs_page
        
        # parse pipeline
        try:
            for func, arg in selector.items():
                if func == 'find':
                    tag_attrs = self.clean_attrs(arg)
                    name_arg, string_arg = self.sort_parse_args(arg)
                    result = result.find(name=name_arg, string=string_arg, attrs=tag_attrs)
                elif func == 'find_all':
                    tag_attrs = self.clean_attrs(arg)
                    name_arg, string_arg = self.sort_parse_args(arg)
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
            logger.debug(f'Current result before attribute error: {result}')
            logger.debug('Moving on!')
            result = ''
        except IndexError:
            logger.debug('Issue with index selection after select/findall method.')
            logger.debug(f'Current result before index error: {result}')
            logger.debug('Moving on!')
            result = ''
        return result

        
    def safe_api_data_get(self, api_response:dict, selector:list):
        '''
        Parses the json response from the api call using the provided selectors.
        In the case of an error, it returns a string.

        Args: 
        api_response(dict): The api response data to be parsed
        selector(list): list of tags to use to parse the api response

        Returns:
        str | int | float: the parsed value or string from the api response
        '''
        result = api_response
        
        # parse pipeline
        try:
            for key in selector:
                result = result[key]

        except AttributeError:
            logger.debug('Invalid api key used')
            logger.debug(f'Current result before invalid key: {result}')
            logger.debug('Moving on!')
            result = ''
        except IndexError:
            logger.debug('Invalid api index key used')
            logger.debug(f'Current result before index error: {result}')
            logger.debug('Moving on!')
            result = ''
        return result
                  
            
    def parse(self, bs_page:BeautifulSoup, website:Website):
        '''
        Parses the data based on predefined tag definitions and yields the parsed data 
        back to the caller function.

        Args: 
        bs_page(BeaurifulSoup): The bs page object to be parsed to get data
        website(Website): Website object containing data necessary for parsing the website it represents.

        Returns/Yields:
        Content: data parsed from the web page 
        '''
        logger.info('Parsing data')
        # get all the books on the page
        books = self.safe_page_data_get(bs_page, website.itemsTag)
        if (books != '') and (len(books) > 0):
            dataset = []
            # loop through all product data
            for book in books:
                endpoint = website.name
                # instantiate the content object
                content = Content(
                    category=self.safe_page_data_get(bs_page, website.categoryTag),
                    title=self.safe_page_data_get(book, website.titleTag),
                    rating=self.safe_page_data_get(book, website.ratingTag),
                    price=self.safe_page_data_get(book, website.priceTag),
                    availability=self.safe_page_data_get(book, website.availabilityTag),
                    link=self.safe_page_data_get(book, website.linkTag)
                )
                if (website.linkTag['r-url']) and (content.link != ''):
                    content.link = '{}/{}'.format(website.url, content.link)
                dataset.append(content)
                # deliver the parsed data to caller function
                if (len(dataset) == RECORDS_BUFFER):
                    yield (endpoint, dataset)
                    del dataset
                    dataset = []
            # release whatever is left
            if (len(dataset) > 0):
                yield (endpoint, dataset)

        else:
            logger.debug('Books missing on the page! Skipping page...')


    def page_pagination(self, session:HTMLSession, link:str, website:Website):
        '''
        get page and all the next pages that follows it

        Generator function that Delivers the page and all the next pages in the page pagination if available.
        
        Args: 
        session(HTMLSession): Session object with which to make the requests
        link(str): The page link to enter and find all the next pages
        website(Website): Website object containing data necessary for parsing the website it represents.

        Returns/Yields:
        BeautifulSoup: page beautiful soup object 
        '''
        # get curr page
        curr_page = self.get_page(session=session, url=link)

        # deliver current page for parsing
        if (curr_page != None):
            yield curr_page
            # loop through all the next pages until theres none left
            while True:
                # get next page url
                logger.debug('getting next page tag')
                next_page_no = self.safe_page_data_get(bs_page=curr_page, selector=website.paginationTag)
                # if there is no next page, break the loop
                if (next_page_no == ''):
                    break
                # check url for current page
                page_no_match = re.search(r'(index|page.*).html', link)
                curr_page_no = page_no_match.group(0)
                    
                # if next page and current page are the same, break the loop
                if (curr_page_no == next_page_no):
                    break
                # find next page link
                next_link = link.replace(curr_page_no, next_page_no)
                # add link to visited links if it isn't the last page
                if (not next_link in self.visited):
                    self.visited.add(next_link)      
                    # get the page
                    logger.info('going to the next page')
                    page = self.get_page(session=session, url=next_link)
                    # deliver the page for parsing
                    if (page != None):
                        yield page
                        curr_page = page
                        link = next_link
                    else:
                        self.visited.discard(next_link)
                        logger.debug('Something went wrong with getting the next page. Possibly connection retries max out')
                        break

                    
    def get_site_page(self, website:Website):
        '''
        Gets the site page and delivers a beautiful object of the page for parsing
        It also handles pagination and url filtering

        Args: 
        website(Website): Website object containing data necessary for parsing the website it represents.

        Returns/Yields:
        BeautifulSoup: page beautiful soup object 
        '''
        # get original url page
        with HTMLSession() as session:
            page = self.get_page(session=session, url=website.url)
            # find all links on the page that fit a certain pattern
            if (page != None):
                target_links = self.safe_page_data_get(page, website.targetPattern)
                # loop through found links
                if (target_links != '') and (len(target_links) > 0):
                    for link in target_links:
                        # get links of target pages
                        link = link.attrs['href']
                        if not website.absoluteUrl:
                            link = '{}/{}'.format(website.url, link)
                        # add link to visited links
                        if (not link in self.visited):
                            self.visited.add(link)
                            # get page and subsequent next pages
                            yield from self.page_pagination(session, link, website)
        
    def crawl(self, website:Website):
        """
        Get pages from website home page and crawl through filtered links

        Args: 
        website(Website): Website object containing data necessary for parsing the website it represents.

        Returns None
        """
        # fetch page
        pages = self.get_site_page(website)
        for page in pages:
            # parse out the page content
            parsed_data = self.parse(page, website)
            for endpoint, dataset in parsed_data:
                # send content to the pipeline
                self.pipeline(endpoint, dataset)


    def pipeline(self, endpoint:str, dataset:list):
        '''
        Cleans data collected by executing defined pipeline functions

        Args: 
        endpoint(str): Name of destination table or file
        dataset(list): list of Content objects containing data from parsing the website page.

        Returns:
        None
        '''
        # go through pipeline
        logger.info('beginning pipeline')

        # execute pipeline processes
        if (dataset != None):
            # create dataframe
            self.items_scraped += len(dataset)
            df = json_normalize(data=[asdict(content) 
                                      for content in dataset])

            # run pipeline steps/stages
            if (self.steps != None) and (len(self.steps) > 0):
                for step in self.steps:
                    # loop through available functions and execute functions that match the step 
                    for func_name, func in self.pipeline_functions.items():
                        if (step == func_name): #or (step in func_name)
                            logger.debug(f'Executing {func_name} on dataset')
                            df = func(df=df, filename=endpoint, obj=self)
                            logger.debug(f'Finished execution of {func_name} on dataset')
                    # print the data if available in the steps
                    if (step == 'print_it'):
                        records = df.to_dict(orient='records')
                        for record in records:
                            logger.info(f'Data: {record}')
        
        logger.info('Pipeline process complete')

    def work(self, name:str):
        '''
        Consumes a site from the queue to crawl and starts the
        crawling process

        Args: 
        name(str): Name of the worker

        Returns:
        None 
        '''
        while not self.work_queue.empty():
            site = self.work_queue.get()
            
            # instantiate the website object
            website = Website(
                name=site['name'],
                url=site['url'],
                targetPattern=site['targetTag'],
                absoluteUrl=site['AbsoluteUrl'],
                paginationTag=site['paginationTag'],
                itemsTag=site['itemsTag'],
                categoryTag=site['categoryTag'],
                titleTag=site['titleTag'],
                ratingTag=site['ratingTag'],
                priceTag=site['priceTag'],
                availabilityTag=site['availabilityTag'],
                linkTag=site['linkTag'],
            )

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

        Args:
        sites(list): list of sites to scrape

        Returns:
        None
        '''
        # add sites to queue
        for site in sites:
            self.work_queue.put(site)

        # # assign workers in different threads
        # if(len(sites) < MAX_WORKERS):
        #     no_workers = len(sites)
        # else:
        #     no_workers = MAX_WORKERS

        # assigning all hands on deck
        no_workers = len(sites)
        del sites

        # setup workers with work to be done
        logger.info(f"Worker(s) starting work...")
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
            exec.map(self.work, range(1, no_workers+1))
        # workers = [threading.Thread(target=self.work, args=(str(i),))
        #                 for i in range(1, no_workers+1)]

        # # start the wokers and wait till they finish
        # for worker in workers:
        #         worker.start()
        # for worker in workers:
        #         worker.join()

        fin = time.perf_counter() - start
        logger.info(f"Total number of pages scraped: {len(self.visited)}")
        logger.info(f"Total number of items scraped: {self.items_scraped}")
        logger.info(f"Worker(s) finished all work in time: {fin:.2f}s")

    def __exit__(self, type, value, traceback):
        '''
        Closes the storage pool on exit from context manager
        '''
        # close the session object
        # self.session.close()

        # close the database connection pool if storage type used was database
        if (STORAGE_TYPE == 'db'):
            close_pool()