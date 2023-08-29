#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger = glueContext.get_logger()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from py4j.java_gateway import java_import
java_import(sc._gateway.jvm,"java.sql.Connection")
java_import(sc._gateway.jvm,"java.sql.DatabaseMetaData")
java_import(sc._gateway.jvm,"java.sql.DriverManager")
java_import(sc._gateway.jvm,"java.sql.SQLException")

# conn = sc._gateway.jvm.DriverManager.getConnection(source_jdbc_conf.get('url'), source_jdbc_conf.get('user'), source_jdbc_conf.get('password'))
conn = sc._gateway.jvm.DriverManager.getConnection('jdbc:postgresql://database-test.cmzmx1cxsoz2.ap-south-1.rds.amazonaws.com:5432/company', 'postgres', 'postgres')

print(conn.getMetaData().getDatabaseProductName())

# call stored procedure, in this case I call sp_start_job
cstmt = conn.prepareCall("CALL test_update()");
# cstmt.setString("job_name", "testjob");
results = cstmt.execute();

conn.close()

