import mysql.connector
#import csv
import os
from io import StringIO
import boto3
from scraper_settings import DB_USER, DB_DATABASE, DB_HOST, DB_PASSWORD
from scraper_settings import STORAGE_TYPE
from scraper_settings import S3_BUCKET_NAME, S3_OUTPUT_DIR
from scraper_settings import OUTPUT_FILE_DIRECTORY
from datetime import datetime
#import json
from utils.log_tool import get_logger

logger = get_logger("WEB_SCRAPER")


def get_storage():
    if (STORAGE_TYPE == 'file'):
        return None
    elif (STORAGE_TYPE == 'db'):
        return DB_Storage()
    elif (STORAGE_TYPE == 'obj'):
        return Obj_Storage()
    else:
        logger.error('Invalid storage type selected')
        return None


def save_to_csv(filename:str, df):
    filename = OUTPUT_FILE_DIRECTORY + filename + '.csv'
    try:
        # append if file exists
        file_exists = os.path.isfile(filename)
        if (file_exists):
            df.to_csv(filename, mode='a', index=False, header=False)
        # if not, create new file
        else:
            df.to_csv(filename, index=False)
    except Exception as e:
        logger.error('Failed to save data to file!!!')
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    else:
        logger.info(filename + ' saved Successfully!')


def save_to_excel(filename:str, df):
    filename = OUTPUT_FILE_DIRECTORY + filename + '.xlsx'
    try:
        # append if file exists
        file_exists = os.path.isfile(filename)
        if (file_exists):
            df.to_excel(filename, mode='a', index=False, header=False)
        # if not, create new file
        else:
            df.to_excel(filename, index=False)
    except Exception as e:
        logger.error('Failed to save data to file!!!')
        logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
    else:
        logger.debug(filename + ' saved Successfully!')



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
        
    def close(self):
        pass


class DB_Storage:
    def __init__(self):#, host, user, password, database):
        self.host = DB_HOST
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.database = DB_DATABASE
        self.cur = None
        self.conn = None
        #self.connect_to_db()

    def connect_to_db(self):
        try:
            print('connecting to database')
            self.conn = mysql.connector.connect(
                host= self.host,
                user= self.user,
                password= self.password,
                database= self.database, 
                charset='utf8',
                buffered=True
            )
        except Exception as e:
            logger.error('Unable to Connect to database!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        else:
            # create cursor
            self.cur = self.conn.cursor()
            self.cur.execute('USE scraping')
            self.create_tables()
            logger.info('Successfully Connected to database!')

    def create_tables(self):
        self.cur.execute('''CREATE TABLE IF NOT EXISTS books(
                         bookId INT NOT NULL AUTO_INCREMENT,
                         title VARCHAR(1000),
                         rating INT,
                         price DECIMAL(6,2),
                         availability VARCHAR(20),
                         category VARCHAR(50),
                         link VARCHAR(1000),
                         created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         PRIMARY KEY(bookId));''')

    def insert_data(self, table_name, df):
        # connect to database if there is no connection
        if (self.cur == None) and (self.conn == None):
            self.connect_to_db()
            
        # insert the data
        if (self.cur != None) and (self.conn != None):
            inserted_records = 0

            # convert dataframe to records
            records_to_insert = df.to_records(index=False).tolist()

            # get table fields
            table_fields = tuple(df.columns.values.tolist())
            table_fields = str(table_fields).replace("'", "")
            #print('data fields are: ', table_fields)

            # get values place holder based on the number of fields being inserted
            number_of_vals = len(records_to_insert[0]) - 1
            vals_placeholder = '%s, ' * number_of_vals + '%s'

            # loop through each record
            for record in records_to_insert:
                #print(record)
                self.cur.reset()
                
                # check if record already exists
                title = record[0].replace('\"', '')
                sql = f'''SELECT bookId 
                        FROM scraping.{table_name} 
                        WHERE title = "{title}"'''
                try:
                    #print(f'Book Title: {title}')
                    self.cur.execute(sql)
                except Exception as e:
                    logger.error('Unable to get row_id!!!')
                    logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                    
                    # save to alternative storage
                    logger.info('Saving to file instead')
                    save_to_csv('failed_data.csv', df)
                    return
                    #print(self.cur.fetchone()[0])
                    
                    # if not, insert new record
                    #print(f'Acquired book id is: {self.cur.fetchone()[0]}')
                    #print(f'Row count: {self.cur.rowcount}')
                else:
                    if (self.cur.rowcount == 0):
                        self.cur.reset()
                        #print(f'current table fields: {table_fields}')
                        sql = f'''INSERT INTO scraping.{table_name} 
                                {table_fields}
                                VALUES ({vals_placeholder})'''
                        #print(sql)
                        try:
                            self.cur.execute(sql, record)
                        except Exception as e:
                            logger.error('Unable to insert data into database!!!')
                            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
                            #self.conn.rollback()
                            
                            # save to alternative storage
                            logger.info('Saving to file instead')
                            save_to_csv('failed_data.csv', df)
                        else:
                            # commit the insertion
                            self.conn.commit()
                            inserted_records += 1
            logger.info(f'{inserted_records} records inserted into database!')
            
        else:
            # save to alternate file if db connection failed
            logger.error('Unable to Connect to database!')
            logger.info('Saving to file instead')
            file_name = table_name + '.csv'
            save_to_csv(file_name, df)  

    def query_all(self, table_name):
        if (self.cur == None) and (self.conn == None):
            self.connect_to_db()
            
        try:
            self.cur.reset()
            sql = f'''SELECT * 
                FROM scraping.{table_name}'''
            print(sql)
            self.cur.execute(sql)
        except Exception as e:
            logger.error('Failed to execute query!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        else:
            result = [row for row in self.cur.fetchall()]
            return result

    def query(self, sql):
        # Run custom sql query
        if (self.cur == None) and (self.conn == None):
            self.connect_to_db()
            
        try:
            self.cur.reset()
            logger.debug(sql)
            self.cur.execute(sql)
        except Exception as e:
            logger.error('Failed to execute query!!!')
            logger.error(f'Exception: {e.__class__.__name__}: {str(e)}')
        else:
            result = [row for row in self.cur.fetchall()]
            return result

    
    def close(self):
        # close connection
        if (self.cur != None):
            self.cur.reset()
            self.cur.close()
        if (self.conn != None):
            self.cur.reset()
            self.conn.close()