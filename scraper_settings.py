import os

''' STORAGE '''
STORAGE_TYPE = 'file'#file','db','obj'
STORAGE = True #False


''' PIPELINE STEPS '''
STEPS = ['remove_nulls', 'clean_title', 'clean_rating', 'clean_price', 'clean_link']


''' WEBSITE '''
WAIT_TIME = 2
WEBSITE_FILE_PATH = 'Websites/'

''' DATABASE '''
DB_HOST = 'localhost' # os.environ.get('DB_HOST')
DB_USER = 'root' # os.environ.get('DB_USER')
DB_PASSWORD = 'Peter-602' # os.environ.get('DB_PASSWORD')
DB_DATABASE = 'mysql' # os.environ.get('DB_DATABASE')


''' OBJECT STORAGE '''
KEY='<key>' # os.environ.get('AWS_KEY')
SECRET='<secret>' # os.environ.get('AWS_SECRET_KEY')
REGION='us-east-1' # os.environ.get('AWS_REGION')

#S3_STAGING_DIR='s3://ebube-scraping_projects/output/' # os.environ.get('S3_STAGING_DIR')
S3_BUCKET_NAME='ScrapingProjects'# ebube-test-bucket # os.environ.get('S3_BUCKET_NAME')
S3_OUTPUT_DIR='Output' # os.environ.get('S3_OUTPUT_DIR')


''' FILE STORAGE '''
OUTPUT_FILE_DIRECTORY = 'Output/' # os.environ.get('FILE_OUTPUT_DIR')


''' PROXIES '''
PROXIES = {
    'http':'socks5://127.0.0.1:9050',
    'https':'socks5://127.0.0.1:9050'
      }