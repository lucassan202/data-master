CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.grupoProblema(
  nomefantasia string, 
  grupoproblema string, 
  datrefcarga string, 
  qtdreclamcoes bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED BY 
  'io.delta.hive.DeltaStorageHandler' 
WITH SERDEPROPERTIES ( 
  'path'='hdfs://namenode:9000/data/gold/grupoproblema/', 
  'serialization.format'='1')
LOCATION
  'hdfs://namenode:9000/data/gold/grupoproblema/'
TBLPROPERTIES (
  'spark.sql.sources.provider'='DELTA', 
  'transient_lastDdlTime'='1716422378');
