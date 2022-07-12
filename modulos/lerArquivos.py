from modulos.sessaoSpark import spark

class readArquivos:

    def lerArquivosCsv(pathCompleto,spark):
        df = spark.read.option("header",True).csv(pathCompleto)    
        return df

    def lerArquivosJson(pathCompleto,spark):
        df = spark.read.option("header",True).json(pathCompleto)    
        return df
