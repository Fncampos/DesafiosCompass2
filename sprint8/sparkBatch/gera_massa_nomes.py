import os
import names
import random
import time

inicio = time.time()
#Grava em csv
def grava_str_em_arquivo(texto, nomeArquivo):
    f = open(nomeArquivo, "a")
    f.write(texto + ",\n")
    f.close()
    print("...")
    
# Define a semente de aleatoriedade
random.seed(40)

#gera 3000 nomes
aux=[]
qtd_nomes_unicos = 3000
for i in range(0, qtd_nomes_unicos):
    aux.append(names.get_full_name())

#Gera lista com 10000000 nomes, tomando como base os nomes gerados anteriormente
dados=[]
qtd_nomes_aleatorios = 10000000
for i in range(0,qtd_nomes_aleatorios):
    dados.append(random.choice(aux))

gravacao = [grava_str_em_arquivo(dado, "nomes_aleatorios.txt") for dado in dados]

fim = time.time()
tempo_execucao = fim - inicio
print(f"Tempo de execução:{tempo_execucao} segundos")