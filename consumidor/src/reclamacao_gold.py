from utils.data import Data
from utils.tables import sConsumidor, gReclamacaotopten
from pyspark.sql.functions import col, count

class ReclamacaoTopTen():
    
    def run(spark, log, datRefCarga):        
        data = Data()

        consumidor = data.read(sConsumidor, datRefCarga, spark, log)

        consumidor = consumidor.groupBy(col('nomefantasia'), col('datRefCarga'))\
          .agg(count(col('nomefantasia')).alias('qtdReclamcoes'))\
          .orderBy(col('qtdReclamcoes'), ascending=False)\
          .limit(10)
        
        data.write(consumidor, gReclamacaotopten, log)