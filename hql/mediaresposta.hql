CREATE EXTERNAL TABLE g_consumidor.mediaresposta(
  nomefantasia string, 
  datrefcarga string, 
  mediaRespostaDias double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED BY 
  'io.delta.hive.DeltaStorageHandler' 
WITH SERDEPROPERTIES ( 
  'path'='hdfs://namenode:9000/data/gold/mediaresposta', 
  'serialization.format'='1')
LOCATION
  'hdfs://namenode:9000/data/gold/mediaresposta'
TBLPROPERTIES (
  'spark.sql.sources.provider'='DELTA', 
  'transient_lastDdlTime'='1716210079');