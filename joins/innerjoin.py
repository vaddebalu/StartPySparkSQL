"""
PySpark program to shows how to perform inner join in PySpark program
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
dept=spark.read.option('header',True).csv('data/dept/dept.csv')
inner=emp.join(dept,on='dno',how='inner')
inner.select(inner['ename'],inner['dname']).show()
