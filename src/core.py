import sqlite3
import logging 
from urllib import request
from hashlib import sha256
import datetime 
import json
import os 

logger = logging.getLogger(__name__)

class Initialize:

    '''
        This class is used for initializing sqlite3 Initialize 'Sweet' 
        for storing companies data containing a list of CIK codes and ticker 
        as well as internal application data for loading application states.
        The list is actively maintained by comparing hashes with older updates.
    '''

    Initialize_path = os.path.curdir + "/db/sweet.db"
    def __init__(self):
        self.conn = None

        if os.path.exists(Initialize.Initialize_path):
            logger.info("Initialize Exists, Creating connection.")

        else:
            logger.warning("Initialize Not Found, Creating a New Initialize.")

        try:
            with sqlite3.connect(Initialize.Initialize_path) as conn:
                self.conn = conn
                logger.info("Connection Successful.")

                # create tables
                self.create_tables()
                return self.conn 

        except Exception as e: 
                logger.error("Initialize Connection Failed: \n",e)
                logger.info(e)
    
    def create_tables(self):
        # Creates tables if it does not exists, if not it will skip.
        logger.info("Creating Tables to Sweet DB")

        try:
            logger.info("Creating cik_lookup Table")

            cik_lookup = """ CREATE TABLE IF NOT EXISTS CIK_LOOKUP (
                        ID INTEGER,
                        CIK VARCHAR(255) NOT NULL,
                        TICKER VARCHAR(10) NOT NULL,
                        TITLE VARCHAR(100) NOT NULL,
                        VIEW_ BOOL NOT NULL,
                        PRIMARY KEY (ID)
                        ); """
            self.conn.execute(cik_lookup)

        except Exception as e:
            logger.error("Failed to Create Lookup Table: \n",e)
        
        try:
            logger.info("Creating update_history Table")
            
            update_history = """ CREATE TABLE IF NOT EXISTS UPDATE_HISTORY (
                            ID INTEGER,
                            LAST_UPDATED DATETIME NOT NULL,
                            CHECKSUM VARCHAR(256) NOT NULL,
                            PRIMARY KEY(ID)
                            );"""
            self.conn.execute(update_history)

        except Exception as e:
            logger.error("Failed to Create Update History Table: \n",e)

class Retrieve:

    '''
        This class is used in retrieving latest CIK codes along with ticker data from SEC site.
        The list is actively maintained by comparing sha-value of last recorded data present to the latest.
        ## step 1: add data using retrieve class 
        ## step 2: if data already exists compare SHA hashes to keep updated
        ##         if changes need to be incorporated mark changes and update rows
    '''

    company_tickers_url = "https://www.sec.gov/files/company_tickers.json"
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q=0.8',
                'Connection': 'keep-alive'}
    
    def __init__(self,db): 
        # sync data from SEC site and compare existing hash
        # case 1: assume if database is fresh and there is no hashes present
        # case 2: if hashes are present compare the new hash and last inserted hash in the database

        # step 1: obtain data and calculate hash (check case 1 and case 2)
        # step 2: put data into database
        try:
            company_ticker_data = request.urlopen(request.Request(Retrieve.company_tickers_url,headers=Retrieve.headers)).read()
            hash_ = sha256(str(company_ticker_data).encode('utf-8')).hexdigest()
            logger.info("Generated Hash for New Data:  ",hash_)

            try:
                company_ticker_data = json.loads(company_ticker_data)
            except Exception as e:
                logger.error("Failed to Convert to Dictionary", e)
        except Exception as e: 
            logger.error("Connection Error: ",e)
    
    def flushToDB(self,db,hash,data):
        # step 1: add the new hash
        pass


class Core:

    """
        This class is the main utility class to interact with sqlite database. 
        The only difference between initialize class and this class is 
        it has CRUD operations to interact with database. 
    """

    def __init__(self):
        self.cursor = Initialize()
        # adds new data to databases using this class
        Retrieve(self.cursor)

    


