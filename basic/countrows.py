"""
PySpark program to display row count from a dataset
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('emp.csv')
emp.count()