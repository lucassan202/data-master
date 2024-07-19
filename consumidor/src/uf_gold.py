from utils.data import Data
from utils.tables import sConsumidor, gReclacaoUf
from pyspark.sql.functions import col, count

class UfProblema():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.read(sConsumidor, datRefCarga, spark, log)

        consumidor = consumidor.groupBy(col('nomefantasia'), col('uf'), col('datRefCarga'))\
                                .agg(count(col('nomefantasia')).alias('qtdReclamcoesUf'))
        
        data.write(consumidor, gReclacaoUf, log)