# Transfere objetos de uma pasta local para Bucket S3 usando um Container Docker com volume Bind Mount e Python com Boto3 e Dotenv
* **Objetivo:** transferir objetos de uma pasta local para Bucket S3 de forma automatizada.
* **Linguagem:** Python
* **Recursos:** Instalações do Docker e bibliotecas Python Boto3 e Dotenv.

## O que o Programa uploadLocal-s3.py faz?

Identifica os arquivos dentro de um volume do tipo Bind Mount, lê as informações do arquivo (extensão, nome e data de criação) e envia para o bucket s3, que é definido estáticamente. Além disso ele cria automaticamente o seguinte padrão de diretório no bucket: data-lake-do-fulano\Raw\Local\CSV\Series\2022\05\02\arquivo.csv 
Após a transferência o programa é finalizado.

## Antes do build da imagem para que funcione como esperado.
### 1. Especifique seu bucket
No arquivo **uploadLocal-s3.py** , linha 10, altere o valor da variável **'bucket_name'** para o nome do bucket destino. Ex: bucket_name = "data-lake" .

### 2. Credenciais
É necessário copiar as credenciais de acesso ao S3 ("Command line or programmatic access") no arquivo **.env**
Exemplo de como é esperado:

    aws_access_key_id= seu-key-id
    aws_secret_access_key= seu-secret-acess-key
    aws_session_token= seu-session-token

## Build da imagem e Execução do container.
1. Abra o terminal e encontre o diretório onde se encontra este projeto. Iremos trabalhar no terminal.
2. Crie a imagem: ' docker build -t ingest . '
3. Você precisará especificar um diretório local onde estão os arquivos que deseja transferir. Nessa pasta devem ter apenas os objetos que você deseja enviar.
Execute o camando: ' docker run -it --name envia-objeto-S3 -v <dir_local>:/app/dados --rm ingest '
4. Arguade até aparecer mensagem de transferência finalizada.

## Ideia de melhoramento:
* Criar uma pasta de transferidos e mover arquivos transferidos para essa pasta de forma automática, com objetivo de manter a pasta 'dados' apenas com os arquivos ainda não enviados.
* Fazer interação com usuário para pedir bucket.
* Incluir try para tratamento de erros.

