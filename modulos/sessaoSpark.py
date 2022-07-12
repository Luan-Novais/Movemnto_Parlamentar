from doctest import FAIL_FAST
from pyspark.sql import SparkSession
from pyspark.shell import spark


def iniciaSpark():
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('Estudo_de_caso')\
            .getOrCreate()
    
    return spark