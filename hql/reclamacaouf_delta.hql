CREATE EXTERNAL TABLE IF NOT EXISTS g_consumidor.reclamacaouf(
  nomefantasia string, 
  uf string,
  datrefcarga string, 
  qtdReclamcoesUf bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED BY 
  'io.delta.hive.DeltaStorageHandler' 
WITH SERDEPROPERTIES ( 
  'path'='hdfs://namenode:9000/data/gold/reclamacaouf', 
  'serialization.format'='1')
LOCATION
  'hdfs://namenode:9000/data/gold/reclamacaouf'
TBLPROPERTIES (
  'spark.sql.sources.provider'='DELTA', 
  'transient_lastDdlTime'='1716210079');