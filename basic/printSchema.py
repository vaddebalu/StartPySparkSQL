"""
PySpark program to display schema from a dataset
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('emp.csv')
emp.printSchema()