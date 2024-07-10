from utils.tables import bConsumidor, tmpCheckpoint
from pyspark.sql.functions import lit, col, substring
from pyspark.sql.types import StructType

class StreamBronze():
    def run(spark, log, pathCsv):
        log.info("Definindo Schema Consumidor")
        schema = StructType() \
	                .add('gestor', 'string') \
                  .add('canalOrigem', 'string') \
                  .add('regiao', 'string') \
                  .add('uf', 'string') \
                  .add('cidade', 'string') \
                  .add('sexo', 'string') \
                  .add('faixaEtaria', 'string') \
                  .add('anoAbertura', 'integer') \
                  .add('mesAbertura', 'integer') \
                  .add('dataAbertura', 'string') \
                  .add('dataResposta', 'string') \
                  .add('dataAnalise', 'string') \
                  .add('dataRecusa', 'string') \
                  .add('dataFinalizacao', 'date') \
                  .add('prazoResposta', 'string') \
                  .add('prazoAnaliseGestor', 'integer') \
                  .add('tempoResposta', 'integer') \
                  .add('nomeFantasia', 'string') \
                  .add('segmentoMercado', 'string') \
                  .add('area', 'string') \
                  .add('assunto', 'string') \
                  .add('grupoProblema', 'string') \
                  .add('problema', 'string') \
                  .add('comoContratou', 'string') \
                  .add('procurouEmpresa', 'string') \
                  .add('respondida', 'string') \
                  .add('situacao', 'string') \
                  .add('avaliacaoReclamacao', 'string') \
                  .add('notaConsumidor', 'integer') \
                  .add('analiseRecusa', 'string')
        
        log.info("Iniciando leitura csv basecompleta")
        inputDF = spark \
          .readStream \
          .option("maxFilesPerTrigger", 1) \
          .option("header", True) \
          .option("sep", ";") \
          .schema(schema) \
          .csv(f"{pathCsv}/basecompleta*.csv") \
          .withColumn("datRefCarga", lit(substring(col('datafinalizacao'),1,7)))
                
        log.info("Iniciando escrita bronze")
        query = inputDF.writeStream \
                .partitionBy('datRefCarga') \
                .format("delta") \
                .option("path", bConsumidor) \
                .option("checkpointLocation", tmpCheckpoint) \
                .start()        

        query.awaitTermination()