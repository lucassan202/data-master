from utils.tables import bConsumidor
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from utils.data import Data

class Bronze():
    def run(spark, log, datRefCarga, pathCsv):
        data = Data()

        schema = StructType() \
	              .add('gestor', 'string') \
                  .add('canalOrigem', 'string') \
                  .add('regiao', 'string') \
                  .add('uf', 'string') \
                  .add('cidade', 'string') \
                  .add('sexo', 'string') \
                  .add('faixaEtaria', 'string') \
                  .add('anoAbertura', 'string') \
                  .add('mesAbertura', 'string') \
                  .add('dataAbertura', 'string') \
                  .add('dataResposta', 'string') \
                  .add('dataAnalise', 'string') \
                  .add('dataRecusa', 'string') \
                  .add('dataFinalizacao', 'string') \
                  .add('prazoResposta', 'string') \
                  .add('prazoAnaliseGestor', 'string') \
                  .add('tempoResposta', 'string') \
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
                  .add('notaConsumidor', 'string') \
                  .add('analiseRecusa', 'string')
        
        log.info(f"Iniciando leitura csv basecompleta {datRefCarga}")
        
        df = spark.read \
                  .schema(schema) \
                  .option("header", True) \
                  .option("sep", ";") \
                  .csv("{}/basecompleta{}.csv".format(pathCsv, datRefCarga))
        
        df = df.withColumn('datRefCarga', lit(datRefCarga))
                        
        data.insert(df, bConsumidor, log)