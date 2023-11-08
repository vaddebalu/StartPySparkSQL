"""
PySpark program to display top 5 rows from a dataset
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('emp.csv')
emp.show(5)