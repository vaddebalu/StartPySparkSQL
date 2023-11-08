"""
This pyspark program explains how to select few columns from  the existing data set.
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
emp.select(emp['ename'],emp['eno']).show()