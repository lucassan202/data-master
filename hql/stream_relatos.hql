CREATE EXTERNAL TABLE IF NOT EXISTS b_consumidor.stream_relatos(
nomeEmpresa string
 ,status string
 ,tempoResposta string
 ,dataOcorrido string
 ,Cidade string
 ,UF string
 ,Relato string
 ,Resposta string
 ,Nota string
 ,Comentario string
)
PARTITIONED BY (date_ingest date) 
STORED AS PARQUET
LOCATION
  'hdfs://namenode:9000/data/consumidor/strem_reclamacoes';