"""
How to run sql queries in pySpark program?
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
empSchema=StructType([StructField('ename',StringType(),True),
                StructField('eno',IntegerType(),True),
                StructField('salary',IntegerType(),True),
                StructField('dno',IntegerType(),True)])
emp=spark.read.option('header',True).schema(empSchema).csv('data/emp/emp.csv')
#create temporary view
emp.createOrReplaceTempView("employee")
#run queries
spark.sql('select * from employee').show()
