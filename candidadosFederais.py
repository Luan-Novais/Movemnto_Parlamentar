from modulos.lerArquivos import readArquivos
from modulos.sessaoSpark import spark

pathCompleto = '/home/luan/Desktop/DADOS/Ano2019.json'
df = readArquivos.lerArquivosJson(pathCompleto,spark)
df.show(1,False)