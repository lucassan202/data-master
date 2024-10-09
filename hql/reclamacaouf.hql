CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.reclamacaouf(
  nomefantasia string, 
  uf string,  
  qtdReclamcoesUf bigint)
PARTITIONED BY (datrefcarga string) 
STORED AS PARQUET
LOCATION
  'hdfs://namenode:9000/data/gold/reclamacaouf';