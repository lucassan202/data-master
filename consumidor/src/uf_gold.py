from utils.data import Data
from utils.tables import sConsumidor, gReclacaoUf
from pyspark.sql.functions import col, count, concat, lit

class UfProblema():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.readTable(sConsumidor, spark, log, datRefCarga)#consumidor = data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.groupBy(col('nomefantasia'), concat(lit('Brasil - '), col('uf')).alias('uf'), col('datRefCarga'))\
                                .agg(count(col('nomefantasia')).alias('qtdReclamcoesUf')) \
                                .select('nomefantasia', 'uf', 'qtdReclamcoesUf','datRefCarga')  
        
        data.insert(consumidor, gReclacaoUf, log)
        #data.write(consumidor, gReclacaoUf, log, spark, datRefCarga)