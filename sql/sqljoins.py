"""
This program shows how to perform joins in PySpark SQL
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
empSchema=StructType([StructField('ename',StringType(),True),
                StructField('eno',IntegerType(),True),
                StructField('salary',IntegerType(),True),
                StructField('dno',IntegerType(),True)])
deptSchema=StructType([StructField('dno',IntegerType(),True),StructField('dname',StringType(),True)])
emp=spark.read.option('header',True).schema(empSchema).csv('data/emp/emp.csv')
dept=spark.read.option('header',True).schema(deptSchema).csv('data/dept/dept.csv')
#create temporary view
emp.createOrReplaceTempView("employee")
dept.createOrReplaceTempView('dept')
#run queries
spark.sql('show databases').show()
spark.sql('show tables').show()
spark.sql('select e.ename,d.dname from employee e join dept d on e.dno=d.dno').show()