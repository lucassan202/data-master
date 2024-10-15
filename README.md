# Projeto Data Master Consumidor Serviços Financeiros

Este projeto tem como objetivo a extração de dados da base do site consumidor.gov.br para análise quantitativa e qualitativa das reclamações geradas por consumidores e clientes de serviços/produtos. 

O projeto tende a abordar mais especificamente reclamações relacionadas a serviços financeiros e instiuições bancárias.

Os dados utilizados são públicos e podem ser extraídos através do site https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br

## Sumário

- [1. Sobre o Consumidor.gov.br](#1-sobre-o-consumidorgovbr)
- [2. Arquitetura](#2-arquitetura)
    - [2.1. Dicionário de dados](#21-dicionário-de-dados)
    - [2.2. Arquitetura](#22-arquitetura)
- [3. Solução Técnica](#3-solução-técnica)    
    - [3.1. Pré Requisitos](#31---pré-requisitos)
    - [3.2. Cluster hadoop-spark](#32-cluster-hadoop-spark)
- [4. Iniciando o projeto](#4-iniciando-o-projeto)
- [5. Realizando consulta SQL](#5-realizando-consulta-sql)
- [6. Execução automatizada Airflow ](#6-execução-automatizada-airflow)
- [7 Monitoramento e observabilidade ](#7-monitoramento-e-observabilidade)
- [8 Dashboards e visualizações dos dados ](#8-dashboards-e-visualizações-dos-dados)
- [9  Pontos de melhoria](#9-pontos-de-melhoria)
- [10 Conclusão ](#10-conclusão)

## 1. Sobre o Consumidor.gov.br

O Consumidor.gov.br é um novo serviço público para solução alternativa de conflitos de consumo por meio da internet, que permite a interlocução direta entre consumidores e empresas, fornece ao Estado informações essenciais à elaboração e implementação de políticas públicas de defesa dos consumidores e incentiva a competitividade no mercado pela melhoria da qualidade e do atendimento ao consumidor.

Trata-se de uma plataforma tecnológica de informação, interação e compartilhamento de dados, monitorada pelos Procons e pela Secretaria Nacional do Consumidor do Ministério da Justiça, com o apoio da sociedade.

## 2. Arquitetura

### 2.1. Dicionário de dados:

O dicionário de dados que o consumidor.gov.br disponibiliza, se encontra na raíz do projeto:

```
dicionario-de-dados-consumidorgovbr-v3.pdf
```
### 2.2. Arquitetura de dados
    
Para o projeto foi utilizada a arquitetura `Medalhão` de dados. Que pode ser ilustrada na figura abaixo:
    
![Databricks Medallion Architecture](/img/medalhao.webp)

Seu principal benefício é realizar uma organização dentro do repositório, distribuindo os dados em várias camadas/níveis diferentes e que ao mesmo tempo possam atender necessidades diferentes do negócio, mantendo o ambiente seguro, organizado e de fácil compreensão.

Além da arquitetura de dados, o projeto conta com ferramentas de observabilidade (`Prometheus`, `Grafana`), orquestração `Airflow` para execução dos pipelines de ingestão de dados e dashboards interativos para visualização dos dados utilizando o `Power BI`.

### 2.3. Arquitetura Sistema

O sistema esta dividido em dois pipelines `batch` e `streaming`. Onde o pipeline batch realiza a extração dos CSVs mensalmente, e o stream que realiza a extração de dados como relatos, respostas e comentários das reclamações do site consumidor.gov.

Abaixo os desenhos das arquiteturas:

#### 2.3.1 Arquitetura Batch

![Arquitetura batch](/img/arquitetura_batch_2.png)

#### 2.3.2 Arquitetura Stream

![Arquitetura stream](/img/arquitetura_stream.png)

## 3. Solução técnica

### 3.1.   Pré requisitos
    
#### 3.1.1 Requisitos mínimos recomendados:
    
- 16 GB RAM
- CPU 4 núcleos 8 treads ou superior

#### 3.1.2  Sofwares

- Docker
- Power BI
- VSCode
    
### 3.2. Cluster hadoop-spark

Como parte da solução foi utilizado um cluster hadoop-spark baseado no projeto `Marcel-Jan/ docker-hadoop-spark` 

```
https://github.com/marcel-jan/docker-hadoop-spark
```

Foram realizadas algumas alterações e melhorias para  adaptação ao cenário proposto. 
    
O cluster pode ser iniciado com o seguinte comando:

```
make deploy_all
```

Após todos os serviços subirem, será necessário algumas configurações caso você deseje executar o spark localmente.

No terminal inspecione a rede `hadoop-spark` e confira qual `IPv4Address` foi atribuido para o`namenode`, este ip deve ser adicionado no seu `etc/hosts` conforme imagens abaixo

Comando para inspecionar e editar o hosts:
```
docker network inspect hadoop-spark
vi /etc/hosts
```
    
![System](/img/namenode_ip.png)
![System](/img/hosts.png)


## 4. Sobre o projeto

### 4.1 run_download

O pipeline batch há 9 procedimentos, começando pelo `run_download` que é responsável por realizar o download do arquivo CSV de uma forma automática, nesta etapa é utilizada uma biblioteca python chamada `beatiful soap` para realizar a extração do html buscando assim o link do arquivo para download, a busca é realizada através da data (dat_ref_carga), passada pelo airflow. Mais a frente irei explicar como a data é inserida no processo.

### 4.2 put_hdfs

O `put_hdfs` é responsável por copiar o arquivo que foi baixado localmente para o hdfs e assim os demais processos conseguirem processa-lo

### 4.3 run_bronze
O `run_bronze` é o processo reponsável pela ingestão e a transformação do arquivo csv em parquet, sem haver nenhum tratamento de dados ou filtros, é ingestado assim como é recebido.

### 4.4 run_silver

O `run_silver` é o processo reponsável pela ingestão, filtro e tratamento dos dados na camada silver a partir do parquet bronze, nele é realizado a filtragem apenas para área de serviços financeiros e bancos.

### 4.5 processamentos gold

Por fim temos 5 processamentos de visoẽs de agrupadas na camada gold onde:
        
- `run_top_ten:` top 10 reclamações no mês específico por instituição.

- `run_grupo_problema:` reúne as categorias dos principais problemas apontados pelos consumidores

- `run_uf:` realiza o agrupamento(contagem) por UF e nome da instituição

- `run_avaliacao` reúne a média agrupada de avaliações dos consumidores por instituição.

- `run_resposta` reúne a média agrupada por tempo de resposta em dias que a instituição leva.


Logo após as inserções na camada gold, podemos visualizar os dados através do hive.


Tabelas para consulta:

```
b_consumidor.consumidor

s_consumidor.consumidorservicosfinanceiros

g_consumidor.grupoProblema

g_consumidor.mediaavaliacao

g_consumidor.mediaresposta

g_consumidor.reclamacaouf
```


## 5. Realizando consulta sql

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

## 6. Execução automatizada Airflow 

Para acessar o airflow utilize o seguinte endereço
```
localhost:9999
```

A dag run_jobs esta programada para rodar mensalmente e realizar a ingestão de M-2. Para execução manual é necessário alterar no airflow no seguinte path `Admin->Variables` a variável `dat_ref_carga` para o ano-mês desejado.

![System](/img/airflow_variables.png)
![System](/img/airflow_edit_variable.png)

Necessário também alterar o path de apontamento do projeto na variável `path_project`

![System](/img/airflow_path_project.png)
    
Execute as dags `run_screp` e `run_jobs`

- Job Screp que realiza a busca dos textos das reclamações e insere em um tópico no `kafka`:

    ![System](/img/airflow_screp.png)
    
- Job Run executa a extração do CSV(download) e ingestões na camada bronze, silver e gold posteriormente:

    ![System](/img/run_jobs_airflow.png)


## 7. Monitoramento e observabilidade

Para este tópico é utilizada duas principais ferrametas `prometheus` e `grafana`.
    
* `Prometheus` É responsável por realizar a coleta de logs e métricas;
* `Grafana` Possibilita a visualização em dashboards das coletas realizadas pelo `prometheus`

Há também os exporters que são utilizados para extrair/coletar as métricas nos serviços de origem como `docker`, `hdfs` etc.

No projeto estamos utilizando os seguintes exporters:

* `hdfs_exporter` - Exporta estatísticas do Hadoop HDFS como número de diretórios, número de arquivos, número de blocos etc.
* `cadvisor` - Exporta dados de telemetria do docker

Para acessar o `grafana`, utilize o seguinte endereço `localhost:3000` usuário `admin` e senha `admin`. Após acessar vá em Connections -> Data source e clique em `+ Add new data source`.

Em Connection adicione a url `http://prometheus:9090` e depois clique em `Save & test`. Pronto a conexão com o prometheus já foi realizada, basta agora importarmos os dashboards para visualizar as métricas.

Na home clique em dashboards e depois no botão `New`, vá na opção `Import`.

- Dashboard hadoop-exporter utilize o código `12236`

    ![System](/img/hadoop-exporter.png)

- Dashboard docker monitoring utilize o código `193`

    ![System](/img/docker-monitor.png)


## 8. Dashboards e visualizações dos dados

Para visualização dos dados é utilizado o `Power BI` através de uma maquina virtual com Windows 10.

Para conexão com o Hive basta selecionar Obter dados e selecionar `LLAP do Hive`. As configurações para conexão são as mesmas encontradas no item [5. Realizando consulta SQL](#5-realizando-consulta-sql).

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

## 9. Pontos de melhoria
### 9.1 A realizar

- A partir dos textos realizar análise de sentimento e inferir notas;
- Implementar integração com ferramentas cloud para escalonamento de recursos;
- Criar outras visualizações gold a partir dos dados silver como faixa etária, como contratou, orgão responsável pela reclamação, se procurou a empresa antes da reclamação, etc;
- Criar alertas nas ferramentas de monitoramento(grafana e airflow), caso haja gargalos de recursos e problemas nas execuções(parcialmente);
- Dimensionar recursos para execução dos processos(parcialmente);

### 9.1 Realizadas
- Criar ferramenta de extração dos textos das reclamações do site consumidor.gov para realizar uma análise mais detalhada dos motivos das reclamações;
- Processo batch para baixar automaticamente arquivo na origem;

## 10. Conclusão

Este projeto oferece uma solução robusta para um case de engenharia de dados, cobrindo todo o processo desde a ingestão dos dados com `Spark-Stream` até a visualização no `Power BI` através de dashboards interativos. 

Ao longo do desenvolvimento, foram abordados temas essenciais como `arquitetura medalhão`, `monitoramento e observabilidade`, e `pipeline de ingestão`, demonstrando a completude e a complexidade de um projeto de engenharia de dados.

Além disso, foram identificados pontos de melhorias que serão implementados em versões futuras, garantindo a evolução contínua do projeto.

