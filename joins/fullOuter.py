"""
PySpark program to shows how to perform full outer join in PySpark program
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
emp=spark.read.option('header',True).csv('data/emp/emp.csv')
dept=spark.read.option('header',True).csv('data/dept/dept.csv')
fulljoin=emp.join(dept,on='dno',how='full')
fulljoin.select(fulljoin['ename'],fulljoin['dname']).show()
