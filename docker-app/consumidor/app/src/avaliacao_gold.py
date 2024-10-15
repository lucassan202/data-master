from utils.data import Data
from utils.tables import sConsumidor, gMediaAvaliacao
from pyspark.sql.functions import col, count, round, sum

class MediaAvaliacao():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.readTable(sConsumidor, spark, log, datRefCarga)#data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.filter(~col('notaconsumidor').isNull())\
                                .groupBy(col('nomefantasia'), col('datRefCarga'))\
                                .agg(round(sum(col('notaconsumidor'))/count(col('nomefantasia')), 2).alias('mediaAvaliacao'))\
                                .select('nomefantasia', 'mediaAvaliacao', 'datRefCarga')  
        
        data.insert(consumidor, gMediaAvaliacao, log)
        #data.write(consumidor, gMediaAvaliacao, log, spark, datRefCarga)