import mysql.connector
from io import StringIO
import boto3
import time
from pandas import DataFrame
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

    def insert_data(self, filename, df:DataFrame):
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
        size = 25
    else:
        if (MAX_WORKERS < 25):
            size = MAX_WORKERS
        else:
            size = 25

    try:
        logger.info(f'Connecting to {DB_NAME} database')
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
        del cnx_pool


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
        sql_create_table = '''
        CREATE TABLE IF NOT EXISTS scraping.BookScrape(
            bookId INT NOT NULL AUTO_INCREMENT,
            title VARCHAR(1000),
            rating INT,
            price_£ DECIMAL(6,2),
            availability VARCHAR(20),
            category VARCHAR(50),
            link VARCHAR(1000),
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(bookId))'''
        # sql_truncate_table = 'TRUNCATE TABLE scraping.BookScrape'
        try:
            with mysql.connector.connect(pool_name='my_pool') as cnx:
                with cnx.cursor() as cur:
                    cur.execute(sql_create_table)
                    # cur.execute(sql_truncate_table)
        except Exception as e:
            logger.error('Failed to execute query!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')


    def insert_data(self, table_name:str, fields:list, records:list):
        global cnx_pool
        # connect to database if there is no connection
        if (cnx_pool == None) or (not cnx_pool.is_connected()):
            self.connect_to_db()
            
        # save to alternative storage if cnx pool is not connected
        if (not cnx_pool.is_connected()):
            logger.error('Unable to Connect to database!')
            logger.info('Saving to file instead')
            file_name = table_name + '.csv'
            # table_fields = table_fields.replace('(', '').replace(')', '')
            # table_fields = table_fields.split(',')
            rec_df = DataFrame(data=records, columns=fields)
            save_to_csv(filename=file_name, df=rec_df)
            return

        records_to_insert:list = []
        #print('data fields are: ', table_fields)

        # convert to string tuple for query statement
        table_fields = str(tuple(fields)).replace("'", "").replace('"', '')

        # get values place holder based on the number of fields being inserted
        number_of_vals = len(records[0]) - 1
        vals_placeholder = '%s, ' * number_of_vals + '%s'
        
        inserted_records = 0
        while True:
            try:
                with mysql.connector.connect(pool_name='my_pool') as cnx:
                    with cnx.cursor() as cur:
                        # loop through each record
                        for record in records:
                            #print(record)
                            # check if record already exists
                            title = record[1].replace('\"', '')
                            sql = f'''SELECT bookId FROM scraping.{table_name} WHERE title = "{title}"'''
                            try:
                                cur.execute(sql)
                            except Exception as e:
                                logger.error(f'Unable to get row_id of record: {record}')
                                logger.debug(sql)
                                logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                                
                                # save to alternative storage
                                logger.info('Saving to file instead')
                                db_df = DataFrame(data=[record,], columns=fields)
                                save_to_csv(filename=f'{table_name}_failed_data', df=db_df)
                            else:
                                # if record exists, add it to the list of records to insert
                                cur.reset()
                                if (cur.rowcount == 0):
                                    records_to_insert.append(record)
                                else:
                                    logger.debug(f'Record with similar title already exits for record: {record}')
                        # insert records to insert into database
                        sql = f'''INSERT INTO scraping.{table_name} {table_fields} VALUES ({vals_placeholder})'''
                        try:
                            cur.executemany(sql, records_to_insert)
                        except Exception as e:
                            logger.error('Unable to insert data into database!!!')
                            logger.debug(sql)
                            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                            cnx.rollback()
                            # save to alternative storage
                            logger.info('Saving to file instead')
                            db_df = DataFrame(data=records_to_insert, columns=fields)
                            save_to_csv(filename=f'{table_name}_failed_data', df=db_df)
                        else:
                            # commit the insertion
                            cnx.commit()
                            inserted_records += len(records_to_insert)
                logger.info(f'{inserted_records} record(s) inserted into database!')
                break
            except mysql.connector.errors.PoolError:
                logger.warning('Current database connection pool exhausted!!!')
                logger.warning('Waiting 3 secs for available connection')
                time.sleep(3)
            except Exception as e:
                logger.error('DB error Occured!!!')
                logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                break

    def query_all(self, table_name):
        global cnx_pool

        if (cnx_pool == None) or (not cnx_pool.is_connected()):
            self.connect_to_db()
         
        with mysql.connector.connect(pool_name='my_pool') as cnx:
            with cnx.cursor() as cur:   
                try:
                    cur.reset()
                    sql = f'''SELECT * FROM scraping.{table_name}'''
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
