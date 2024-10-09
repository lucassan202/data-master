from utils.data import Data
from utils.tables import sConsumidor, gGrupoproblema
from pyspark.sql.functions import col, count
from pyspark.sql.types import DoubleType

class GrupoProblema():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.readTable(sConsumidor, spark, log, datRefCarga)#consumidor = data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.groupBy(col('nomefantasia'), col('grupoProblema'),col('datRefCarga'))\
                                .agg(count(col('nomefantasia')).alias('qtdReclamcoes'))\
                                .select('nomefantasia', 'grupoProblema', 'qtdReclamcoes', 'datRefCarga')        
        
        data.insert(consumidor, gGrupoproblema, log)
        #data.write(consumidor, gGrupoproblema, log, spark, datRefCarga)