import hashlib

while True:
    dado = input("Digite uma string para gerar seu hash SHA-1 (ou digite 'sair' para encerrar o programa): ")
    if dado.lower() == "sair":       
        print("Encerrando o programa...")
        break
    hash = hashlib.sha1(dado.encode())
    print("O hash SHA-1 da string é:", hash.hexdigest(),"\n")
