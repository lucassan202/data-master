from utils.functions import Functions
from utils.data import Data
from utils.tables import bConsumidor, sConsumidor
from pyspark.sql.functions import col, lit, when, from_unixtime, unix_timestamp
from pyspark.sql.types import DateType, BooleanType, IntegerType

class Silver():
    
    def run(spark, log, datRefCarga):
        functions = Functions()
        data = Data()

        columns = ['dataAbertura', 'dataResposta', 'dataAnalise', 'dataRecusa'
                   , 'prazoResposta', 'procurouEmpresa', 'respondida', 'anoAbertura'
                   , 'mesAbertura', 'dataFinalizacao', 'prazoAnaliseGestor'
                   , 'tempoResposta', 'notaConsumidor']
        
        expressions = [ from_unixtime(unix_timestamp(col('dataAbertura'), 'dd/MM/yyyy')).cast(DateType())
               , from_unixtime(unix_timestamp(col('dataResposta'), 'dd/MM/yyyy')).cast(DateType()) 
               , from_unixtime(unix_timestamp(col('dataAnalise'), 'dd/MM/yyyy')).cast(DateType()) 
               , from_unixtime(unix_timestamp(col('dataRecusa'), 'dd/MM/yyyy')).cast(DateType())               
               , from_unixtime(unix_timestamp(col('prazoResposta'), 'dd/MM/yyyy')).cast(DateType())
               , when(col('procurouEmpresa') == 'S', lit(1)).otherwise(lit(0)).cast(BooleanType())
               , when(col('respondida') == 'S', lit(1)).otherwise(lit(0)).cast(BooleanType())
               , col('anoAbertura').cast(IntegerType())
               , col('mesAbertura').cast(IntegerType())
               , col('dataFinalizacao').cast(DateType())
               , col('prazoAnaliseGestor').cast(IntegerType())
               , col('tempoResposta').cast(IntegerType())
               , col('notaConsumidor').cast(IntegerType())
              ]
                        
        consumidor = data.readTable(bConsumidor, spark, log, datRefCarga)

        consumidor = functions.qualifyTypeColumn(consumidor, columns, expressions)

        consumidor = consumidor.filter((col('nomefantasia').like('Banco%')) & (col('area')=='Serviços Financeiros') )        

        data.insert(consumidor, sConsumidor, log)