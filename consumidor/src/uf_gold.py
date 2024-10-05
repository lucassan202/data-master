from utils.data import Data
from utils.tables import sConsumidor, gReclacaoUf
from pyspark.sql.functions import col, count, concat, lit

class UfProblema():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.groupBy(col('nomefantasia'), concat(lit('Brasil - '), col('uf')).alias('uf'), col('datRefCarga'))\
                                .agg(count(col('nomefantasia')).alias('qtdReclamcoesUf'))
        
        data.write(consumidor, gReclacaoUf, log, spark, datRefCarga)