from modulos.sessaoSpark import spark

class ler_arquivos:

    def ler_arquivo_csv(pathCompleto,spark):
        df = spark.read.option("header",True).csv(pathCompleto)    
        return df

    def ler_arquivo_json(pathCompleto,spark):
        df = spark.read.option("multiline","true").json(pathCompleto)    
        return df
