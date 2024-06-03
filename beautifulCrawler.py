from bs4 import BeautifulSoup
import re
import time
#import requests
#import aiohttp
import httpx
import asyncio
import glob
import json
from scraper_settings import STEPS, STORAGE_TYPE
from scraper_settings import PROXIES, MAX_WORKERS, WAIT_TIME
from scraper_settings import WEBSITE_FILE_PATH
from inspect import getmembers, isfunction
import pipeline_funcs
from content import Content, Dataset
from storage import get_storage, close_pool, get_pool
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

class Crawler:
    def __init__(self):
        self.site: Website
        #self.session = requests.Session()
        #self.session = aiohttp.ClientSession()
        # self.session = httpx.AsyncClient(event_hooks={'request':[self.log_request],
        #                                               'response':[self.log_response]})
        self.session = None
        self.visited: list = []
        self.storage = get_storage()
        self.pipeline_functions: dict = funcs
        self.steps: list[str] = STEPS
        self.proxies_available: bool = self.check_for_proxies()
        self.work_queue = asyncio.Queue()

    async def __aenter__(self):

        await get_pool()
        self.session = httpx.AsyncClient(event_hooks={'request':[self.log_request],
                                                      'response':[self.log_response]})
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
        
    def safe_get(self, pageObj, selector):
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
                
    async def parse_page_data(self, bs):
        '''
        Parses the data based on predefined tag definitions. The pipeline function is
        then executed to clean and possibly store the acquired dataset.
        Returns None
        '''

        #logger.info('#' * 25)
        logger.info('Parsing data')
        # parse the data
        category = self.safe_get(bs, self.site.categoryTag)
        products = self.safe_get(bs, self.site.itemsTag)

        if (products == '') or (category == ''):
            logger.debug('Category or Products tag missing! Skipping category...')
            return
            
        # instantiate the dataset object
        dataset = Dataset(endpoint= self.site.name)
        
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
            #logger.debug(content)
            dataset.records.append(content)
        # save data to file
        #save_to_csv(dataset.endpoint, dataset.dataframe())
        #save_to_excel(dataset.endpoint, dataset.dataframe())
              
        # send data to pipeline
        await self.pipeline(dataset)
                  
            
    async def parse(self, url):
        '''
        Recursive function that isolates next page link and parseable data. 
        Calls the parse function and procedes to the next page, if it exists.
        Returns None
        '''
        # get page
        page = await self.get_page(url)
        if page is not None:
            # get book/product links
            await self.parse_page_data(page)
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
            next_page = self.safe_get(page, self.site.paginationTag)
            if (next_page != ''):
                # check url for current page
                page_match = re.search(r'(index|page.*).html', url)
                curr_page = page_match.group(0)
                
                # if next page exists, go to the next page
                if (curr_page != next_page):
                    url = url.replace(curr_page, next_page)
                    # get the page data
                    logger.info('going to next page')
                    await self.parse(url)
        
    async def crawl(self, website):
        """
        Get pages from website home page and crawl through filtered links
        Returns None
        """
        # start_time = time.perf_counter()
        self.site = website
        bs = await self.get_page(self.site.url)
        if (bs != None):
            # get target pages
            targetPages = self.safe_get(bs, self.site.targetPattern)

            # loop through pages to get data
            if (targetPages != '') and (len(targetPages) > 0):
                for targetPage in targetPages:
                    #logger.info('-' * 50)
                    # get links of target pages
                    targetPage = targetPage.attrs['href']
                    
                    # add to visited list if not in there
                    if targetPage not in self.visited:
                        self.visited.append(targetPage)
                        if not self.site.absoluteUrl:
                            targetPage = '{}/{}'.format(self.site.url, targetPage)
                            
                        # parse page data
                        await self.parse(targetPage)
                        #logger.info('-' * 50)
        # end_time = time.perf_counter()
        # logger.info(f"Exhausted time: {end_time - start_time:.2f}s")
        # logger.info(f"Crawling Complete!!")

    async def pipeline(self, dataset):
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
            if (self.steps != None) and (len(self.steps) > 0):
                for step in self.steps:
                    for func_name, func in self.pipeline_functions.items():
                        if (step == func_name): #or (step in func_name)
                            logger.debug(f'Executing {func_name} on dataset')
                            df = func(df=df, filename=dataset.endpoint, obj=self)
                            logger.debug(f'Finished execution of {func_name} on dataset')

            # # store the data
            if (self.storage != None):
                logger.info(f'saving data to {STORAGE_TYPE} storage: {dataset.endpoint}')
                await self.storage.insert_data(dataset.endpoint, df)
            else:
                records = df.to_dict(orient='records')
                for record in records:
                    logger.info(f'Data: {record}')
        
        logger.info('Pipeline process complete')
        #logger.info('>'*25)

    async def run(self):
        # create an empty list to contain the sites
        num_of_sites = 0

        # get website settings in json files
        logger.info('Gathering website configuration files...')
        for jsonfile in glob.glob(WEBSITE_FILE_PATH + "*.json"):
            with open(jsonfile, 'r') as openfile: 
                # Reading from json file 
                json_object = json.load(openfile)
                await self.work_queue.put(json_object)
                num_of_sites += 1

        logger.info(f'Gathered {num_of_sites} website files')
        # record start timer
        start_time = time.perf_counter()
            
        # determine number of workers
        if (num_of_sites > MAX_WORKERS):
            if(MAX_WORKERS <= 0):
                num_workers = 1
            else:
                num_workers = MAX_WORKERS
        else:
            num_workers = num_of_sites
        
        try:
            # start workers
            tasks = [asyncio.create_task(self.task(str(index))) 
                    for index in range(1, num_workers+1)]
            # done, pending = await asyncio.wait(tasks)
            await asyncio.wait(tasks)

        except Exception as e:
            logger.error('Failed to execute query!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        finally:
            end_time = time.perf_counter()
            logger.info(f"Process completed in time: {end_time - start_time: .2f}s")
            # clean up async shit that can't be done in the exit func
            #await self.clean_up()
        

    async def task(self, name):
        '''
        Defines the task to be handled by a worker. It can be crawling or scraping
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
            logger.info(f"Worker-{name} starts task of crawling {website.name}")
            start_time = time.perf_counter()
            # async with httpx.AsyncClient(event_hooks={'request':[self.log_request],
            #                                           'response':[self.log_response]}) as client:
            # self.session = client
            await self.crawl(website)
            fin = time.perf_counter() - start_time
            logger.info(f"Worker-{name} completed task in time: {fin: .2f}s")
        

    # async def clean_up(self):
    #     # close the storage
    #     #await close_pool()
    #     if (self.storage != None) and (STORAGE_TYPE == 'db'):
    #         await self.storage.close()
    #         self.storage = None


    async def __aexit__(self, type, value, traceback):

        # close the connection pool
        await close_pool()

        # close the session
        await self.session.aclose()

        
        # close the storage
        if (self.storage != None) and (STORAGE_TYPE == 'db'):
            await self.storage.close()
            self.storage = None
        else:
            self.storage.close()
