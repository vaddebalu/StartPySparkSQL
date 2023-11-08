"""
PySpark program to shows how to perform cross join in PySpark program
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
dept=spark.read.option('header',True).csv('data/dept/dept.csv')
crossjoin=emp.join(dept,on='dno',how='cross')
crossjoin.select(crossjoin['ename'],crossjoin['dname']).show()