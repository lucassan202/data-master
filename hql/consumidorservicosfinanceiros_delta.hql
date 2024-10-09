CREATE EXTERNAL TABLE IF NOT EXISTS s_consumidor.consumidorservicosfinanceiros(
  gestor string, 
  canalorigem string, 
  regiao string, 
  uf string, 
  cidade string, 
  sexo string, 
  faixaetaria string, 
  anoabertura int, 
  mesabertura int, 
  dataabertura date, 
  dataresposta date, 
  dataanalise date, 
  datarecusa date, 
  datafinalizacao date, 
  prazoresposta date, 
  prazoanalisegestor int, 
  temporesposta int, 
  nomefantasia string, 
  segmentomercado string, 
  area string, 
  assunto string, 
  grupoproblema string, 
  problema string, 
  comocontratou string, 
  procurouempresa boolean, 
  respondida boolean, 
  situacao string, 
  avaliacaoreclamacao string, 
  notaconsumidor int, 
  analiserecusa string, 
  datrefcarga string,
  datproc timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED BY 
  'io.delta.hive.DeltaStorageHandler' 
WITH SERDEPROPERTIES ( 
  'path'='hdfs://namenode:9000/data/consumidor/silver', 
  'serialization.format'='1')
LOCATION
  'hdfs://namenode:9000/data/consumidor/silver'
TBLPROPERTIES (
  'spark.sql.sources.provider'='DELTA', 
  'transient_lastDdlTime'='1716142206');
