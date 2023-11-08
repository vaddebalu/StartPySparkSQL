"""
PySpark program to shows how to perform min aggregate
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
empSchema=StructType([StructField('ename',StringType(),True),
                StructField('eno',IntegerType(),True),
                StructField('salary',IntegerType(),True),
                StructField('dno',IntegerType(),True)])
emp=spark.read.option('header',True).schema(empSchema).csv('data/emp/emp.csv')

emp.groupby('dno').min('salary').show()