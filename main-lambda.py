#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import logging
import psycopg2
import json
import os
from psycopg2.extras import RealDictCursor

user_name = "postgres"
password_new = "postgres"
rds_host = "database-test.cmzmx1cxsoz2.ap-south-1.rds.amazonaws.com"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn = psycopg2.connect(host=rds_host, user=user_name, password=password_new, port = "5432", database="company")
logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")


def lambda_handler(event, context):
    """
    This function creates a new RDS database table and writes records to it
    """
    Name = 'aditya'
    item_count = 0
    
    CustID = 0
    with conn.cursor() as cur:
        #cur.execute("create table if not exists customer ( CustID  int NOT NULL, Name varchar(255) NOT NULL, PRIMARY KEY (CustID))")
        #cur.execute("SELECT pg_sleep(600)")
        cur.execute(" create or replace procedure test_update() language plpgsql as $$ begin update Customer set name = 'solanki' where custid  = 41; end;$$;")
        #cur.execute(" create or replace procedure test_insert() language plpgsql as $$ begin insert into Customer (CustID, Name) values(31, 'New Proc'); end;$$;")
        # cur.execute("Call test_update ()")
        cur.execute("select * from Customer")
        for row in cur:
            item_count += 1
            logger.info(row)
            
        cur.execute("select max(CustID) from Customer")
        new = 0
        for row in cur:
            new = (row[0]) + 1
        CustID = new
        # CustID = 1
        sql_string = f"insert into Customer (CustID, Name) values({CustID}, '{Name}')"
        cur.execute(sql_string)
        
    conn.commit()

    return "Added %d items to RDS MySQL table" %(item_count)

