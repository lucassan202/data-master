from utils.tables import bConsumidor, tmpCheckpoint
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, ArrayType, DateType

class Stream():
    def run(spark, log):
        log.info("Definindo Schema")

        Schema = ArrayType(StructType() \
	                .add('nomeEmpresa', 'string') \
                  .add('status', 'string') \
                  .add('tempoResposta', 'string') \
                  .add('dataOcorrido', 'string') \
                  .add('Cidade', 'string') \
                  .add('UF', 'string') \
                  .add('Relato', 'string') \
                  .add('Resposta', 'string') \
                  .add('Nota', 'string') \
                  .add('Comentario', 'string'))
        
        log.info("Leitura tópico kafka")
        inputDF = spark \
          .readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "kafka:9092") \
          .option("subscribe", "reclamacoes") \
          .load()
        
        lstRelatos = inputDF.withColumn('date_ingest', col('timestamp').cast(DateType())) \
                            .select(from_json(
                                            col('value').cast('String')
                                            , Schema
                                        ).alias("data"), 'date_ingest'
                                    )\
                            .select(explode('data').alias('data'), 'date_ingest') \
                            .select('data.*', 'date_ingest')


        log.info("Iniciando escrita")
        
        query = lstRelatos.writeStream \
            .partitionBy("date_ingest") \
            .format("parquet") \
            .option("path", "hdfs://namenode:9000/data/consumidor/strem_reclamacoes") \
            .option("checkpointLocation", tmpCheckpoint) \
            .start()         

        query.awaitTermination()