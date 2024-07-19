# Projeto Data Master Consumidor Serviços Financeiros

Este projeto tem como objetivo a extração de dados da base do site consumidor.gov.br para análise quantitativa e qualitativa das reclamações geradas por consumidores e clientes de serviços/produtos. 

O projeto tende a abordar mais especificamente reclamações relacionadas a serviços financeiros e instiuições bancárias.

Os dados utilizados são públicos e podem ser extraídos através do site https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br

## Sobre o Consumidor.gov.br

O Consumidor.gov.br é um novo serviço público para solução alternativa de conflitos de consumo por meio da internet, que permite a interlocução direta entre consumidores e empresas, fornece ao Estado informações essenciais à elaboração e implementação de políticas públicas de defesa dos consumidores e incentiva a competitividade no mercado pela melhoria da qualidade e do atendimento ao consumidor.

Trata-se de uma plataforma tecnológica de informação, interação e compartilhamento de dados, monitorada pelos Procons e pela Secretaria Nacional do Consumidor do Ministério da Justiça, com o apoio da sociedade.

## Arquitetura

* Dicionário de dados:

    O dicionário de dados que o consumidor.gov.br disponibiliza se encontra na raíz do projeto:

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

    *   pré requisitos
    
        - O projeto foi desenvolvido com as seguintes configurações, não sendo recomendado menos recursos do que estes.

        ![System](/img/system.png)

        - Docker
        - Anaconda 1.12.3 com python 3.8
        - Airflow standalone
        - PowerBI
        - VSCode
    
    * Cluster hadoop-spark

        Como parte da solução foi utilizado um cluster  hadoo-spark baseado no projeto `Marcel-Jan/  docker-hadoop-spark` 

        ```
        https://github.com/marcel-jan/docker-hadoop-spark
        ```

        Foram realizadas algumas alterações e melhorias para    adaptação ao cenário proposto. 
        
        O cluster pode ser  iniciado com o seguinte comando:

        ```
            cd docker-hadoop-spark/
            docker-compose up
        ```
        Após todos os serviços subirem, será necessário algumas configurações.

        No terminal inspecione a rede `hadoop-spark` e confira qual `IPv4Address` foi atribuido para o `namenode`, este ip deve ser adicionado no seu `/etc/hosts`

        ![System](/img/namenode_ip.png)
        ![System](/img/hosts.png)

        ```
        docker network inspect hadoop-spark
        vi /etc/host
        ```