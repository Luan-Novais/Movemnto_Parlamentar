from modulos.lerArquivos import ler_arquivos
from modulos.sessaoSpark import spark

path_completo = '/home/luan/Desktop/DADOS/teste2.json'
#path_completo = '/home/luan/Desktop/arq1.csv'

df = ler_arquivos.ler_arquivo_json(path_completo,spark)
df.show(2,False)