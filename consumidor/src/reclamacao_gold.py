from utils.data import Data
from utils.tables import sConsumidor, gReclamacaotopten
from pyspark.sql.functions import col, count

class ReclamacaoTopTen():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.readTable(sConsumidor, spark, log, datRefCarga)#consumidor = data.read(sConsumidor, spark, log, True)

        consumidor = consumidor.groupBy(col('nomefantasia'), col('datRefCarga'))\
          .agg(count(col('nomefantasia')).alias('qtdReclamcoes'))\
          .orderBy(col('qtdReclamcoes'), ascending=False)\
          .select('nomefantasia', 'qtdReclamcoes', 'datRefCarga') \
          .limit(10)
        
        data.insert(consumidor, gReclamacaotopten, log)
        #data.write(consumidor, gReclamacaotopten, log, spark, datRefCarga)