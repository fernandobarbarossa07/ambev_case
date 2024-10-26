# Projeto Airflow com Docker

Este projeto usa o Apache Airflow em contêineres Docker para orquestrar workflows definidos em scripts Python. Após a configuração, o usuário poderá acessar o Airflow via interface web no endereço `http://localhost:8080` e visualizar o DAG `orquestra_jobs`, que é a orquestração do projeto.

## Pré-requisitos

Antes de iniciar, certifique-se de que o **Docker** está instalado na sua máquina. Você pode instalar o Docker seguindo as instruções disponíveis no [site oficial do Docker](https://docs.docker.com/get-docker/).

## Configuração Inicial

Para garantir que os arquivos de jobs tenham permissões adequadas para serem executados dentro dos contêineres, execute o comando abaixo no diretório raiz do projeto:

```bash
chmod +x ./Job1.py ./Job2.py ./Job3.py
``````
Inicie o **Docker**
```bash
docker-compose up
````
Acesse o endereço, e procure pela DAG **orquestra_jobs**

```bash
http://localhost:8080
````
Depois de rodar, desative o docker
```bash
docker-compose down
````
##  Explicação do projeto
Três Jobs são instanciados: Job1, Job2 e Job3.
### Job1
Tem por função fazer um request a API **https://api.openbrewerydb.org/v1/breweries**, fazendo uma paginação para obter todos os dados e o salvando na camada **BRONZE** no formato JSON, inserindo também o ano, mês e dia no qual o objeto foi salvo.

### Job2
Tem por função ler o arquivo JSON da camada **BRONZE** e o normalizar, se tornando um DataFrame. Além disso é criado o campo de data de ingestão do dado, e o campo `country` é tratado para não ter espaços, pois ele se tornará um campo de partição ao salvar o dado na camada **SILVER**, junto com o campo de data (o fluxo opera como se fosse um fluxo diário). Os arquivos são salvos no formato **parquet**.

### Job3
Tem por função ler todos os parquets, fazendo um laço na pasta **SILVER** dentro do folder do dia atual. Com isso, concatena todos os DataFrames em um só. Tendo apenas um arquivo, gera-se a **Smart Table** para ser salva na camada **GOLD**. A tabela resume-se em uma contagem vinda de um agrupamento pelo tipo de cervejaria por localização (país). Após isso, o dado é salvo.