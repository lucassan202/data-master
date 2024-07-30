# Projeto Data Master Consumidor Serviços Financeiros

Este projeto tem como objetivo a extração de dados da base do site consumidor.gov.br para análise quantitativa e qualitativa das reclamações geradas por consumidores e clientes de serviços/produtos. 

O projeto tende a abordar mais especificamente reclamações relacionadas a serviços financeiros e instiuições bancárias.

Os dados utilizados são públicos e podem ser extraídos através do site https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br

## Sobre o Consumidor.gov.br

O Consumidor.gov.br é um novo serviço público para solução alternativa de conflitos de consumo por meio da internet, que permite a interlocução direta entre consumidores e empresas, fornece ao Estado informações essenciais à elaboração e implementação de políticas públicas de defesa dos consumidores e incentiva a competitividade no mercado pela melhoria da qualidade e do atendimento ao consumidor.

Trata-se de uma plataforma tecnológica de informação, interação e compartilhamento de dados, monitorada pelos Procons e pela Secretaria Nacional do Consumidor do Ministério da Justiça, com o apoio da sociedade.

## Arquitetura

* Dicionário de dados:

    O dicionário de dados que o consumidor.gov.br disponibiliza, se encontra na raíz do projeto:

    ```
    dicionario-de-dados-consumidorgovbr-v3.pdf
    ```
* Arquitetura
    
    Para o nosso projeto foi utilizada a arquitetura `Medalhão` de dados. Que pode ser ilustrada na figura abaixo:
    
    ![Databricks Medallion Architecture](/img/medalhao.webp)

    Seu principal benefício é realizar uma organização dentro do repositório, distribuindo os dados em várias camadas/níveis diferentes e que ao mesmo tempo possam atender necessidades diferentes do negócio, mantendo o ambiente seguro, organizado e de fácil compreensão.

    Além da arquitetura de dados, o projeto conta com ferramentas de observabilidade (`Prometheus`, `Grafana`) e orquestração `Airflow` para execução dos pipelines de ingestão de dados.

## Solução técnica

* Infra-estrutura

*   Pré requisitos
    
    - O projeto foi desenvolvido com as seguintes configurações, não sendo recomendado menos recursos do que estes.

        ![System](/img/system.png)

    - Docker
    - Anaconda 1.12.3 com python 3.8
    - Airflow standalone
    - PowerBI
    - VSCode
    
* Cluster hadoop-spark

    Como parte da solução foi utilizado um cluster hadoop-spark baseado no projeto `Marcel-Jan/ docker-hadoop-spark` 

    ```
    https://github.com/marcel-jan/docker-hadoop-spark
    ```

    Foram realizadas algumas alterações e melhorias para  adaptação ao cenário proposto. 
    
    O cluster pode ser iniciado com o seguinte comando:

    ```
    cd docker-hadoop-spark/
    docker-compose up
    ```

    Após todos os serviços subirem, será necessário algumas configurações.

    No terminal inspecione a rede `hadoop-spark` e confira qual `IPv4Address` foi atribuido para o`namenode`, este ip deve ser adicionado no seu `etc/hosts` conforme imagens abaixo

    Comando para inspecionar e editar o hosts:
    ```
    docker network inspect hadoop-spark
    vi /etc/hosts
    ```
    
    ![System](/img/namenode_ip.png)
    ![System](/img/hosts.png)
    
    Após é possível iniciar o projeto executando via bash no terminal, mais a frente irei mostrar comoexecutar de forma automatizada utilizando o`Airflow` no item x.xx

## Iniciando o projeto

Primeiramente devemos iniciar nosso spark stream que irá realizar a ingestão do csv do consumidor.gov.br na nossa camada bronze. Por default o path onde o processo irá buscar o csv é o `/consumidor/csv` sendo possível altera-lo na shell `/consumidor/shell/run.sh` variável `csv_path`.

```
bash /consumidor/shell/run.sh stream_bronze
```
Este processo fica aguardando novos arquivos caírem no diretório para realizar a ingestão na camada bronze.

O seguinte processo é a ingestão, filtro e tratamento dos dados na camada silver, nele é realizado a filtragem apenas para área de serviços financeiros e bancos. O segundo parametro da shell é a data de extração do arquivo csv. Ex.: 2024-05

```
bash /consumidor/shell/run.sh silver 2024-05
```
Por fim temos 5 visoẽs de agrupamento na camada gold onde:
        
- `Reclamação Top Ten:` top 10 reclamações no mês específico por instituição.

- `Grupo problema:` reúne as categorias dos principais problemas apontados pelos consumidores

- `Agrupamento por UF:` realiza o agrupamento(contagem) por UF e nome da instituição

- `Média Avaliação:` reúne a média agrupada de avaliações dos consumidores por instituição.

- `Média Tempo de Resposta:` reúne a média agrupada por tempo de resposta em dias que a instituição leva.

```
bash /consumidor/shell/run.sh grupo_problema 2024-05

bash /consumidor/shell/run.sh top_ten 2024-05

bash /consumidor/shell/run.sh avaliacao 2024-05

bash /consumidor/shell/run.sh resposta 2024-05

bash /consumidor/shell/run.sh uf 2024-05
```

Logo após as inserções na camada gold, podemos visualizar os dados através do hive, mas para isso é necessário criar as tabelas apontando para os diretórios da camada gold. Abaixo o script para criação das tabelas.

```
docker exec -it hive-server bash

bash /opt/hql/create_tables.sh
```

Tabelas criadas após a execução do script:

```
b_consumidor.consumidor

s_consumidor.consumidorservicosfinanceiros

g_consumidor.grupoProblema

g_consumidor.mediaavaliacao

g_consumidor.mediaresposta

g_consumidor.reclamacaouf
```


## Realizando consulta sql

Para consulta dos dados recomendo instalar a extensão `SQLTools Hive Driver` no `VSCode`

![System](/img/sqltools.png)

Abaixo as configurações de conexão hive:

```
Connection name: hive
Host: localhost
Port: 10000
Username: scott
Password: tiger
Hive CLI Connection Protocol Version: V10
Show records default limit: 50
```
Exemplo de query para consultar os dados da camada silver:
```
select * from s_consumidor.consumidorservicosfinanceiros where datrefcarga='2024-05' limit 10;
```
Resultado da consulta:
![System](/img/sql_result.png)

## Execução automatizada Airflow 
Para execução do airflow standalone utilize os comandos abaixo. Recomendo a utilização da porta 9998 pois a porta 8080 já esta sendo utilizada pelo `spark master`
```
airflow webserver --port 9998

airflow scheduler
```
Copie as dags do projeto para dentro do path default do `airflow`
```
export MY_DIR=$(cd $(dirname "${0}"); pwd)

cp ./airflow/dags/*.py $MY_DIR/airflow/dags
```

A dag run_jobs esta programada para rodar mensalmente e realizar a ingestão da data atual. Para execução manual é necessário alterar no arquivo `run_jobs.py` a variável `dat_ref_carga` para o ano-mês desejado.

Necessário também alterar o path de apontamento do projeto na variável `path_project`

![System](/img/run_jobs_var.png)
    
Acesse o endereço `http://localhost:9998/` e execute as dags `run_stream` e `run_jobs`

Job stream que realiza a inserção do csv na camada bronze:
![System](/img/run_stream.png)
    
Execução da ingestão na camada silver e gold posteriormente:
![System](/img/run_jobs.png)


## Monitoramento e observabilidade

Para este tópico é utilizada duas principais ferrametas `prometheus` e `grafana`.
    
* `Prometheus` É responsável por realizar a coleta de logs e métricas;
* `Grafana` Possibilita a visualização em dashboards das coletas realizadas pelo `prometheus`

Há também os exporters que são utilizados para extrair/coletar as métricas nos serviços de origem como `docker`, `hdfs` etc.

No projeto estamos utilizando os seguintes exporters:

* `hdfs_exporter` - Exporta estatísticas do Hadoop HDFS como número de diretórios, número de arquivos, número de blocos etc.
* `node_exporter` - Exporta dados de telemetria de um nó especifico 
* `cadvisor` - Exporta dados de telemetria do docker

Para acessar o `grafana`, utilize o seguinte endereço `localhost:3000` usuário `admin` e senha `admin`. Após acessar vá em Connections -> Data source e clique em `+ Add new data source`.

Em Connection adicione a url `http://prometheus:9090` e depois clique em `Save & test`. Pronto a conexão com o prometheus já foi realizada, basta agora importarmos os dashboards para visualizar as métricas.

Na home clique em dashboards e depois no botão `New`, vá na opção `Import`.

- Dashboard hadoop-exporter utilize o código `12236`

    ![System](/img/hadoop-exporter.png)

- Dashboard docker monitoring utilize o código `193`

    ![System](/img/docker-monitor.png)

- Dashboard node-exporter utilize o código `1860`

    ![System](/img/node-exporter.png)

## Dashboards e visualizações dos dados

Para visualização dos dados é utilizado o `Power BI` através de uma maquina virtual com Windows 10.

Para conexão com o Hive basta selecionar Obter dados e selecionar `LLAP do Hive`. As configurações para conexão são as mesmas encontradas no item x.xx.

Você pode importar as visualizações pré prontas no seguinte diretório do projeto:

```
/powerbi/Data Master Consumidor.pbix
```

- Quantidade de reclamações por instituição

     ![System](/img/quantidade_reclamacoes.png)

- Quantidade de reclamações por instituição v2

     ![System](/img/quantidade_reclamacoes_v2.png)

- Quantidade de reclamações por Assunto, "nuvem de palavras"

     ![System](/img/nuvem_palavras.png)

- Quantidade de reclamações por UF

     ![System](/img/reclamacoes_uf.png)

- Média de avaliação por instituição

     ![System](/img/media_avaliacao.png)

- Média tempo de resposta por instituição

     ![System](/img/tempo_medio_reposta.png)     