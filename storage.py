import mysql.connector
from io import StringIO
import boto3
from scraper_settings import DB_USER, DB_NAME, DB_HOST, DB_PASSWORD, DB_PORT
from scraper_settings import STORAGE_TYPE, MAX_WORKERS
from scraper_settings import S3_BUCKET_NAME, S3_OUTPUT_DIR
from pipeline_funcs import save_to_csv
from datetime import datetime
#import json
from utils.log_tool import get_logger

logger = get_logger("WEB_SCRAPER")


def get_repo():
    if (STORAGE_TYPE == 'db'):
        return DB_Storage()
    elif (STORAGE_TYPE == 'obj'):
        return Obj_Storage()
    else:
        logger.error('Invalid repository type selected')
        return None





class Obj_Storage:
    def __init__(self):#, host, user, password, database):
        self.bucket = S3_BUCKET_NAME
        self.output_dir = S3_OUTPUT_DIR
        self.s3 = None
        try:
            #s3_resource = boto3.resource('s3')
            self.s3 = boto3.resource('s3',
                                     #region_name=REGION,
                                     #aws_access_key_id=KEY,
                                     #aws_secret_access_key=SECRET
                                    )
        except Exception as e:
            logger.error('Unable to connect to object storage!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    def insert_data(self, filename, df):
        # add prefix with timestamp
        timestamp = datetime.now()
        prefix = f'/scp_{timestamp}/'
        key = self.output_dir + prefix + filename + '.csv'
        csv_buffer = StringIO()
        if (df != None):
            df.to_csv(csv_buffer)
            try:
                self.s3.Object(self.bucket, key).put(Body=csv_buffer.getvalue())
            except Exception as e:
                logger.error('Failed to insert data into bucket')
                logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')

    def get_items(self, prefix):
        try:
            bucket = self.s3.Bucket(self.bucket)
        except Exception as e:
            logger.error('Unable to get items in bucket')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        else:
            data_files = [filename.key for filename in bucket.objects.filter(Prefix=prefix)] # list object files based on prefix
            return data_files
        


cnx_pool = None

def get_pool():
    '''
    Creates a modular connection pool variable for database connections
    '''
    global cnx_pool

    if (cnx_pool != None) and (cnx_pool.is_connected()):
        return
    
    if (MAX_WORKERS == None):
        size = 5
    else:
        size = MAX_WORKERS + 1

    try:
        logger.info(f'Connecting to database {DB_NAME}')
        cnx_pool = mysql.connector.connect(pool_name='my_pool', 
                                        pool_size=size,
                                        host= DB_HOST,
                                        user= DB_USER,
                                        password= DB_PASSWORD,
                                        database= DB_NAME,
                                        port = DB_PORT,
                                        charset='utf8',
                                        buffered=True
                                        )
    except Exception as e:
        logger.error('Unable to Connect to database!!!')
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    else:
        logger.info('Successfully Connected to database!')

def close_pool():
    global cnx_pool
    if (cnx_pool != None):
        cnx_pool.disconnect()
        cnx_pool = None


class DB_Storage:
    def __init__(self):#, host, user, password, database):
        #self.connect_to_db()
        pass

    def connect_to_db(self):
        '''
        Create a connection pool if not available
        '''
        get_pool()

        self.create_tables()

    def create_tables(self):
        global cnx_pool
        sql = '''CREATE TABLE IF NOT EXISTS BookScrape(
                         bookId INT NOT NULL AUTO_INCREMENT,
                         title VARCHAR(1000),
                         rating INT,
                         price_£ DECIMAL(6,2),
                         availability VARCHAR(20),
                         category VARCHAR(50),
                         link VARCHAR(1000),
                         created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         PRIMARY KEY(bookId));'''
        with mysql.connector.connect(pool_name='my_pool') as cnx:
            with cnx.cursor() as cur:
                cur.execute('USE scraping')
                cur.execute(sql)

    def insert_data(self, table_name, df):
        global cnx_pool
        # connect to database if there is no connection
        if (cnx_pool == None) or (not cnx_pool.is_connected()):
            self.connect_to_db()
            
        # insert the data
        if (cnx_pool.is_connected()):

            # convert dataframe to records
            records_to_insert = df.to_records(index=False).tolist()

            # get table fields
            table_fields = tuple(df.columns.values.tolist())
            table_fields = str(table_fields).replace("'", "")
            #print('data fields are: ', table_fields)

            # get values place holder based on the number of fields being inserted
            number_of_vals = len(records_to_insert[0]) - 1
            vals_placeholder = '%s, ' * number_of_vals + '%s'
            
            inserted_records = 0
            with mysql.connector.connect(pool_name='my_pool') as cnx:
                with cnx.cursor() as cur:
                    # loop through each record
                    for record in records_to_insert:
                        #print(record)
                        cur.reset()
                        
                        # check if record already exists
                        title = record[1].replace('\"', '')
                        sql = f'''SELECT bookId 
                                FROM scraping.{table_name} 
                                WHERE title = "{title}"'''
                        logger.debug(sql)
                        try:
                            #print(f'Book Title: {title}')
                            cur.execute(sql)
                        except Exception as e:
                            logger.error('Unable to get row_id!!!')
                            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                            
                            # save to alternative storage
                            logger.info('Saving to file instead')
                            save_to_csv(filename=f'{table_name} failed_data.csv', df=df)
                            return
                        else:
                            if (cur.rowcount == 0):
                                cur.reset()
                                #print(f'current table fields: {table_fields}')
                                sql = f'''INSERT INTO scraping.{table_name} 
                                        {table_fields}
                                        VALUES ({vals_placeholder})'''
                                logger.debug(sql)
                                try:
                                    cur.execute(sql, record)
                                    # commit the insertion
                                    cnx.commit()
                                    inserted_records += 1
                                except Exception as e:
                                    logger.error('Unable to insert data into database!!!')
                                    logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                                    cnx.rollback()
                                    
                                    # save to alternative storage
                                    logger.info('Saving to file instead')
                                    save_to_csv(filename=f'{table_name} failed_data.csv', df=df)
            logger.info(f'{inserted_records} records inserted into database!')
            
        else:
            # save to alternate file if db connection failed
            logger.error('Unable to Connect to database!')
            logger.info('Saving to file instead')
            file_name = table_name + '.csv'
            save_to_csv(filename=file_name, df=df)   

    def query_all(self, table_name):
        global cnx_pool

        if (cnx_pool == None) or (not cnx_pool.is_connected()):
            self.connect_to_db()
         
        with mysql.connector.connect(pool_name='my_pool') as cnx:
            with cnx.cursor() as cur:   
                try:
                    cur.reset()
                    sql = f'''SELECT * 
                        FROM scraping.{table_name}'''
                    logger.debug(sql)
                    cur.execute(sql)
                except Exception as e:
                    logger.error('Failed to execute query!!!')
                    logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                else:
                    result = [row for row in self.cur.fetchall()]
                    return result

    def query(self, sql):
        '''
        Run a custom sql query
        '''
        global cnx_pool

        if (cnx_pool == None) or (not cnx_pool.is_connected()):
            self.connect_to_db()
         
        with mysql.connector.connect(pool_name='my_pool') as cnx:
            with cnx.cursor() as cur: 
                try:
                    cur.reset()
                    logger.debug(sql)
                    cur.execute(sql)
                except Exception as e:
                    logger.error('Failed to execute query!!!')
                    logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                else:
                    result = [row for row in self.cur.fetchall()]
                    return result
