from pyspark.sql import SparkSession
from pyspark.shell import spark


def inicia_spark():
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('Estudo_de_caso')\
            .getOrCreate()
    
    return spark