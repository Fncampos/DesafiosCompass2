import random

#gera 250 numeros aleatórios
def gerar_250_numeros(inicio, fim):
    lista=[]
    for _ in range(250):
        numero=random.randint(inicio, fim)
        lista.append(numero)
    return lista

numeros=gerar_250_numeros(inicio=100, fim=1000)

print(f"numeros gerados: {numeros}")

numeros.reverse()

print(f"numeros em ordem de geração decrescente: {numeros}")



