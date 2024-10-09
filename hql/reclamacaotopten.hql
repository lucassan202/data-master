CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.reclamacaotopten(
  nomefantasia string,   
  qtdreclamcoes bigint)
PARTITIONED BY (datrefcarga string) 
STORED AS PARQUET
LOCATION
  'hdfs://namenode:9000/data/gold/reclamacaotopten/';