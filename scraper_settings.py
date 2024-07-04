import os

''' WORKERS '''
MAX_WORKERS:int = 25
RECORDS_BUFFER:int = 20

''' STORAGE '''
STORAGE_TYPE:str = 'db'#'db','obj', 'csv', 'excel'


''' PIPELINE STEPS '''
STEPS:list[str] = ['remove_nulls', 'clean_title', 'clean_rating', 
                   'clean_price', 'clean_link']


''' WEBSITE '''
MAX_RETRIES:int = 4
WAIT_TIME:float = 2
WEBSITE_FILE_PATH:str = 'Websites/'

''' DATABASE '''
DB_HOST:str = 'localhost' # os.environ.get('DB_HOST')
DB_USER:str = 'root' # os.environ.get('DB_USER')
DB_PASSWORD:str = 'Peter-602' # os.environ.get('DB_PASSWORD')
DB_NAME:str = 'mysql' # os.environ.get('DB_DATABASE')
DB_PORT:int = 3306


''' AWS '''
KEY:str='<key>' # os.environ.get('AWS_KEY')
SECRET:str='<secret>' # os.environ.get('AWS_SECRET_KEY')
REGION:str='us-east-1' # os.environ.get('AWS_REGION')

''' AWS OBJECT STORAGE '''
#S3_STAGING_DIR:str='s3://ebube-scraping_projects/output/' # os.environ.get('S3_STAGING_DIR')
S3_BUCKET_NAME:str='ScrapingProjects'# ebube-test-bucket # os.environ.get('S3_BUCKET_NAME')
S3_OUTPUT_DIR:str='Output' # os.environ.get('S3_OUTPUT_DIR')


''' FILE STORAGE '''
OUTPUT_FILE_DIRECTORY:str = 'Output/' # os.environ.get('FILE_OUTPUT_DIR')


''' MASKING '''
PROXIES:dict = {
    # 'http':'socks5://127.0.0.1:9050',
    # 'https':'socks5://127.0.0.1:9050'
      }
HEADERS:dict = {
    # 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
    # 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    # 'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8'
    }