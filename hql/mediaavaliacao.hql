CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.mediaavaliacao(
  nomefantasia string, 
  mediaAvaliacao double)
PARTITIONED BY (datrefcarga string) 
STORED AS PARQUET
LOCATION
  'hdfs://namenode:9000/data/gold/mediaavaliacao';
