from modulos.lerArquivos import ler_arquivos
from modulos.sessaoSpark import spark
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType,IntegerType

path_completo = '/home/luan/Desktop/DADOS/BRONZE/2023/teste2.json'

df_princial = ler_arquivos.ler_arquivo_json(path_completo,spark)
df_tratado = df_princial.select(col('nomeParlamentar').alias('NOME_PARLAMENTAR'),
                                col('ANO').cast(IntegerType()),
                                col('MES').cast(IntegerType()),
                                col('DESCRICAO'),
                                col('FORNECEDOR'),
                                col('LEGISLATURA').cast(IntegerType()),
                                col('VALORDOCUMENTO').cast(DoubleType()).alias('VALOR_DOCUMENTO'),
                                col('VALORGLOSA').cast(DoubleType()).alias('VALOR_NAO_PAGO'),
                                col('VALORLIQUIDO').cast(DoubleType()).alias('VALOR_LIQUIDO'))

df_tratado.write.mode('overwrite').parquet("/home/luan/Desktop/DADOS/SILVER/2023/")

def regras_de_negocio(spark):
    df_principal = spark.read.load('/home/luan/Desktop/DADOS/SILVER/2023/*.parquet')
    df_valor_ano = df_principal.groupBy(col('NOME_PARLAMENTAR'),col('ANO')).agg(sum(col('VALOR_DOCUMENTO')).alias('VALOR_ANUAL'))
    df_final = df_principal.join(df_valor_ano,['NOME_PARLAMENTAR','ANO'],'inner')
    df_final.write.mode('overwrite').parquet("/home/luan/Desktop/DADOS/GOLDE/2023/")

regras_de_negocio(spark)