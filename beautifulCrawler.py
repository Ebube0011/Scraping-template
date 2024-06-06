from bs4 import BeautifulSoup
import re
import time
#import requests
import httpx
import asyncio
from scraper_settings import STEPS, STORAGE_TYPE
from scraper_settings import PROXIES, MAX_WORKERS, WAIT_TIME
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

# get pipeline functions
p_funcs = getmembers(pipeline_funcs, isfunction)

# add to dictionary
funcs = {}
for p_func in p_funcs:
    func_name, func = p_func
    funcs[func_name] = func

#user_agent = get_user_agent()

class BeautifulCrawler:
    def __init__(self):
        #self.session = requests.Session()
        self.session = httpx.AsyncClient(event_hooks={'request':[self.log_request],
                                                      'response':[self.log_response]})
        self.visited = set()
        self.storage = get_repo()
        self.pipeline_functions: dict = funcs
        self.steps: list[str] = STEPS
        self.proxies_available: bool = self.check_for_proxies()
        self.work_queue = asyncio.Queue()

        # add storage func to pipeline steps
        if (STORAGE_TYPE == 'db') and (not 'save_to_db' in self.steps):
            self.steps.append('save_to_db')
        elif (STORAGE_TYPE == 'obj') and (not 'save_to_obj' in self.steps):
            self.steps.append('save_to_obj')
        elif (STORAGE_TYPE == 'excel') and (not 'save_to_excel' in self.steps):
            self.steps.append('save_to_excel')
        elif (STORAGE_TYPE == 'csv') and (not 'save_to_csv' in self.steps):
            self.steps.append('save_to_csv')

    async def __aenter__(self):
        '''
        Things to initailize when using context managers
        '''
        if (STORAGE_TYPE == 'db'):
            await get_pool()
            await self.storage.create_tables()
        return self
    
    async def log_request(self, request):
        logger.debug(f'Request: {request.method} {request.url}')

    
    async def log_response(self, response):
        request = response.request
        logger.info(f'Response: {request.method} {request.url} - Status {response.status_code}')
    
    
    async def get_page(self, url):
        '''
        Gets the html of the page and converts it to beautiful soup
        Returns beautiful soup object or None if page was no found
        '''
        try:
            await asyncio.sleep(WAIT_TIME)
            logger.info(f'Getting page: {url}')
            response = await self.session.get(url, 
                                              #allow_redirects=True
                                              )
        # except requests.exceptions.RequestException as e:
        #     logger.error('Unable to get page!!!')
        #     logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        #     return None
        except Exception as e:
            logger.error('Unable to get page!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        else:
            un_authorized = [401, 403]
            server_error = [500, 501]
            if (response.status_code == 200):
                html = response.text
                return BeautifulSoup(html, 'lxml')
            elif (response.status_code in server_error):
                logger.info(f'Unable to get page({url}) due to server error')
                return None
            elif (response.status_code in un_authorized):
                logger.info(f'Unable to get page({url}) due to being UnAuthorized')
                logger.info(f'Attempting to get page with masking')
                self.anon_get_page(url)

    def check_for_proxies(self):
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

    async def anon_get_page(self, url):
        '''
        Gets the html of the page anonymously and converts it to beautiful soup
        Returns beautiful soup object or None if page was no found
        '''
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
                   'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                   'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8'
                  }
    
        #requests.get('https://icanhazip.com', proxies=proxies).text.strip()
        # get the webpage
        try:
            await asyncio.sleep(WAIT_TIME)
            logger.info(f'Getting page: {url}')
            if(self.proxies_available):
                proxies = PROXIES
            else:
                proxies = None

            response = await self.session.get(url, 
                                    headers=headers, 
                                    proxies=proxies,
                                    #allow_redirects=True,
                                )
        # except requests.exceptions.RequestException as e:
        #     logger.info('Unable to get page!!!')
        #     logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        #     return None
        except Exception as e:
            logger.info('Unable to get page!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
            return None
        else:
            server_error = [500, 501]
            if (response.status_code == 200):
                html = response.text
                return BeautifulSoup(html, 'lxml')
            elif (response.status_code in server_error):
                logger.info(f'Unable to get page({url}) due to server error')
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
                
    async def parse_page_data(self, bs, website:Website):
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
        dataset = Dataset(endpoint= website.name)
        
        # loop through all product data
        for product in products:
            # instantiate the content object
            content = Content(category= category,
                              title= self.safe_get(product, website.titleTag),
                              rating = self.safe_get(product, website.ratingTag),
                              price = self.safe_get(product, website.priceTag),
                              availability = self.safe_get(product, website.availabilityTag),
                              link = self.safe_get(product, website.linkTag)
            )
            if (website.linkTag['r-url']) and (content.link != ''):
                content.link = '{}/{}'.format(website.url, content.link)
                
            # add product data to the content dataset
            #logger.debug(content)
            dataset.records.append(content)
        # save data to file
        #save_to_csv(dataset.endpoint, dataset.dataframe())
        #save_to_excel(dataset.endpoint, dataset.dataframe())
              
        # send data to pipeline
        await self.pipeline(dataset)
                  
            
    async def parse(self, url:str, website:Website):
        '''
        Recursive function that isolates next page link and parseable data. 
        Calls the parse function and procedes to the next page, if it exists.
        Returns None
        '''
        # get page
        page = await self.get_page(url)
        if page is not None:
            # get book/product links
            await self.parse_page_data(page, website)
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
                    await self.parse(url, website)
        
    async def crawl(self, website:Website):
        """
        Get pages from website home page and crawl through filtered links
        Returns None
        """
        # start_time = time.perf_counter()
        bs = await self.get_page(website.url)
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
                        await self.parse(targetPage, website)
                        #logger.info('-' * 50)
        # end_time = time.perf_counter()
        # logger.info(f"Exhausted time: {end_time - start_time:.2f}s")
        # logger.info(f"Crawling Complete!!")

    async def pipeline(self, dataset:Dataset):
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
            df = dataset.dataframe()

            # run pipeline steps/stages
            print_data = True
            if (self.steps != None) and (len(self.steps) > 0):
                for step in self.steps:
                    for func_name, func in self.pipeline_functions.items():
                        if (step == func_name): #or (step in func_name)
                            logger.debug(f'Executing {func_name} on dataset')
                            df = await func(df=df, filename=dataset.endpoint, obj=self)
                            logger.debug(f'Finished execution of {func_name} on dataset')
                        if ('save' in func_name):
                            print_data = False

            # store the data
            if (print_data):
                records = df.to_dict(orient='records')
                for record in records:
                    logger.info(f'Data: {record}')
        
        logger.info('Pipeline process complete')
        #logger.info('>'*25)

    async def delegate_and_run_work(self, sites:list):
        '''
        Creates crawling workers to work concurrently, on threads, to crawl 
        the provided websites
        '''

        for site in sites:
            await self.work_queue.put(site)
        

        # start workers
        tasks = [asyncio.create_task(self.task(str(index))) 
                for index in range(1, MAX_WORKERS+1)]
        # done, pending = await asyncio.wait(tasks)
        logger.info(f"Worker(s) starting work...")
        
        # record start timer
        start = time.perf_counter()

        await asyncio.wait(tasks)


        duration = time.perf_counter() - start
        logger.info(f"Worker(s) finished all work in time: {duration:.2f}s")
        # clean up async shit that can't be done in the exit func
        #await self.clean_up()
        

    async def task(self, name):
        '''
        Consumes a site from the queue to crawl and starts the
        crawling/scraping process
        '''

        while not self.work_queue.empty():
            # get site data from the work queue
            site = await self.work_queue.get()
            # instantiate the website object
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
            # do the actual task of crawling/scraping
            logger.info(f"Worker-{name} crawling {website.name}")
            start = time.perf_counter()
            await self.crawl(website)
            duration = time.perf_counter() - start
            logger.info(f"Worker-{name} completed task in time: {duration: .2f}s")
        

    async def __aexit__(self, type, value, traceback):
        '''
        Closes the storage pool on exit from context manager
        '''

        # close the connection pool
        await close_pool()

        # close the session
        await self.session.aclose()
