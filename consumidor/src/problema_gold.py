from utils.data import Data
from utils.tables import sConsumidor, gGrupoproblema
from pyspark.sql.functions import col, count

class GrupoProblema():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.groupBy(col('nomefantasia'), col('grupoProblema'),col('datRefCarga'))\
                                .agg(count(col('nomefantasia')).alias('qtdReclamcoes'))
        
        data.write(consumidor, gGrupoproblema, log, spark, datRefCarga)