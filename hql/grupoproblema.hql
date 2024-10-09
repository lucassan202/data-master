CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.grupoProblema(
  nomefantasia string, 
  grupoproblema string,   
  qtdreclamcoes bigint)
PARTITIONED BY (datrefcarga string) 
STORED AS PARQUET
LOCATION
  'hdfs://namenode:9000/data/gold/grupoproblema/';