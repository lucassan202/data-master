CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.mediaresposta(
  nomefantasia string, 
  mediaRespostaDias double)
PARTITIONED BY (datrefcarga string) 
STORED AS PARQUET
LOCATION
  'hdfs://namenode:9000/data/gold/mediaresposta';