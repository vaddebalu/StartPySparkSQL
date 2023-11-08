"""
PySpark program to shows how to perform right outer join in PySpark program
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
dept=spark.read.option('header',True).csv('data/dept/dept.csv')
rightjoin=emp.join(dept,on='dno',how='right')
rightjoin.select(rightjoin['ename'],rightjoin['dname']).show()
