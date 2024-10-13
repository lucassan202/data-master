from utils.data import Data
from utils.tables import sConsumidor, gMediaReposta
from pyspark.sql.functions import col, count, round, sum

class MediaResposta():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.readTable(sConsumidor, spark, log, datRefCarga)#consumidor = data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.filter(col('respondida')==1)\
                                .groupBy(col('nomefantasia'), col('datRefCarga'))\
                                .agg(round(sum(col('temporesposta'))/count(col('nomefantasia')), 0).alias('mediaRespostaDias'))\
                                .select('nomefantasia', 'mediaRespostaDias', 'datRefCarga')  
        
        data.insert(consumidor, gMediaReposta, log)
        #data.write(consumidor, gMediaReposta, log, spark, datRefCarga)