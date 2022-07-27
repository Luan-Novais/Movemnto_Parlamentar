import os

arquivos_raw = os.listdir('/home/luan/Desktop/DADOS/RAW/2022/')

for item in arquivos_raw:
    path_raw = f'/home/luan/Desktop/DADOS/RAW/2022/{item}'
    path_bronze = f'/home/luan/Desktop/DADOS/BRONZE/2022/{item}'

    with  open(path_raw,'r',encoding='utf-8') as arq:
        data = arq.read()
        arquivo_tratado = (data.replace('{\n' '    "dados": [' ,"[")
                              .replace('\n' '}',"\n")) 
        arq.close()
        with open(path_bronze,'x') as arq2:
            arq2.write(arquivo_tratado)
            arq2.close()

# gravando_arquivo_tratado(path_bronze)