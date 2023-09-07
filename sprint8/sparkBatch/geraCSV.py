import random

#Grava em csv
def grava_str_em_arquivo(texto, nomeArquivo):
    f = open(nomeArquivo, "a")
    f.write(texto + ",\n")
    f.close()
    
animais = ["Pinguim","Jacaré","Cavalo-marinho","Coruja","Gorila","Baleia","Papagaio", "Escorpião","Raposa","Esquilo",
           "Canguru","Golfinho","Polvo","Pinguim", "Raposa","Pantera","Pavão","Morcego","Gavião","Rena"]

animais_order= sorted(animais)

imprimi = [grava_str_em_arquivo(animal, "animais.csv") for animal in animais_order]
