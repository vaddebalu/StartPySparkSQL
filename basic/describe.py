"""
PySpark program to compute basic statistics for  a dataset
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('emp.csv')
#describe() computes basic stats
emp.describe().show()