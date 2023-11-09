"""
This program shows how to create global temp view in pyspark sql
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
empSchema=StructType([StructField('ename',StringType(),True),
                StructField('eno',IntegerType(),True),
                StructField('salary',IntegerType(),True),
                StructField('dno',IntegerType(),True)])
emp=spark.read.option('header',True).schema(empSchema).csv('data/emp/emp.csv')
#create temporary global view
emp.createGlobalTempView("empnew")
#run queries
spark.sql('show databases').show()
spark.sql('show tables').show()
#create new spark session and try to access empnew table from that
spark.newSession().sql('select * from global_temp.empnew').show()