import os

''' WORKERS '''
MAX_WORKERS:int = 3

''' STORAGE '''
STORAGE_TYPE:str = 'db'#'db','obj', 'csv', 'excel'


''' PIPELINE STEPS '''
STEPS:list[str] = ['remove_nulls', 'clean_title', 'clean_rating', 
         'clean_price', 'clean_link']


''' WEBSITE '''
WAIT_TIME:float = 2.5
WEBSITE_FILE_PATH:str = 'Websites/'

''' DATABASE '''
DB_HOST:str = 'localhost' # os.environ.get('DB_HOST')
DB_USER:str = 'root' # os.environ.get('DB_USER')
DB_PASSWORD:str = 'Peter-602' # os.environ.get('DB_PASSWORD')
DB_NAME:str = 'mysql' # os.environ.get('DB_DATABASE')
DB_PORT:int = 3306


''' OBJECT STORAGE '''
KEY='<key>' # os.environ.get('AWS_KEY')
SECRET='<secret>' # os.environ.get('AWS_SECRET_KEY')
REGION='us-east-1' # os.environ.get('AWS_REGION')

#S3_STAGING_DIR='s3://ebube-scraping_projects/output/' # os.environ.get('S3_STAGING_DIR')
S3_BUCKET_NAME='ScrapingProjects'# ebube-test-bucket # os.environ.get('S3_BUCKET_NAME')
S3_OUTPUT_DIR='Output' # os.environ.get('S3_OUTPUT_DIR')


''' FILE STORAGE '''
OUTPUT_FILE_DIRECTORY:str = 'Output/' # os.environ.get('FILE_OUTPUT_DIR')


''' PROXIES '''
PROXIES = {
    'http':'socks5://127.0.0.1:9050',
    'https':'socks5://127.0.0.1:9050'
      }