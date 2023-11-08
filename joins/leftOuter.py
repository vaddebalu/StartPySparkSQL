"""
PySpark program to shows how to perform left outer join in PySpark program
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
dept=spark.read.option('header',True).csv('data/dept/dept.csv')
leftjoin=emp.join(dept,on='dno',how='left')
leftjoin.select(leftjoin['ename'],leftjoin['dname']).show()