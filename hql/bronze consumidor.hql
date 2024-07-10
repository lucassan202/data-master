CREATE EXTERNAL TABLE b_consumidor.consumidor(
  gestor string, 
  canalorigem string, 
  regiao string, 
  uf string, 
  cidade string, 
  sexo string, 
  faixaetaria string, 
  anoabertura int, 
  mesabertura int, 
  dataabertura string, 
  dataresposta string, 
  dataanalise string, 
  datarecusa string, 
  datafinalizacao date, 
  prazoresposta string, 
  prazoanalisegestor int, 
  temporesposta int, 
  nomefantasia string, 
  segmentomercado string, 
  area string, 
  assunto string, 
  grupoproblema string, 
  problema string, 
  comocontratou string, 
  procurouempresa string, 
  respondida string, 
  situacao string, 
  avaliacaoreclamacao string, 
  notaconsumidor int, 
  analiserecusa string, 
  datrefcarga string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED BY 
  'io.delta.hive.DeltaStorageHandler' 
WITH SERDEPROPERTIES ( 
  'path'='hdfs://namenode:9000/data/consumidor/bronze', 
  'serialization.format'='1')
LOCATION
  'hdfs://namenode:9000/data/consumidor/bronze'
TBLPROPERTIES (
  'spark.sql.sources.provider'='DELTA', 
  'transient_lastDdlTime'='1716056661');
