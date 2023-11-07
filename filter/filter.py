"""
Sample pyspark program that applies a filter on a dataset
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
emp.filter(emp['deptno']>12).show()