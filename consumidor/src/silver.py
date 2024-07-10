from utils.functions import Functions
from utils.data import Data
from utils.tables import bConsumidor, sConsumidor
from pyspark.sql.functions import col, lit, when, from_unixtime, unix_timestamp
from pyspark.sql.types import DateType, BooleanType

class Silver():
    
    def run(spark, log, datRefCarga):
        functions = Functions()
        data = Data()

        columns = ['dataAbertura', 'dataResposta', 'dataAnalise', 'dataRecusa'
                   , 'prazoResposta', 'procurouEmpresa', 'respondida']
        
        expressions = [ from_unixtime(unix_timestamp(col('dataAbertura'), 'dd/MM/yyyy')).cast(DateType())
               , from_unixtime(unix_timestamp(col('dataResposta'), 'dd/MM/yyyy')).cast(DateType()) 
               , from_unixtime(unix_timestamp(col('dataAnalise'), 'dd/MM/yyyy')).cast(DateType()) 
               , from_unixtime(unix_timestamp(col('dataRecusa'), 'dd/MM/yyyy')).cast(DateType())               
               , from_unixtime(unix_timestamp(col('prazoResposta'), 'dd/MM/yyyy')).cast(DateType())
               , when(col('procurouEmpresa') == 'S', lit(1)).otherwise(lit(0)).cast(BooleanType())
               , when(col('respondida') == 'S', lit(1)).otherwise(lit(0)).cast(BooleanType())
              ]
                        
        consumidor = data.read(bConsumidor, datRefCarga, spark, log)

        consumidor = functions.qualifyTypeColumn(consumidor, columns, expressions)

        consumidor = consumidor.filter((col('nomefantasia').like('Banco%')) & (col('area')=='Serviços Financeiros') )

        data.write(consumidor, sConsumidor, log)        