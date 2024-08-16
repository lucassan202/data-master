from delta.tables import DeltaTable
from pyspark.sql.functions import col

class Data:
    def read(self, table, spark, log, maxPartition=False, partition=None):
        if maxPartition:
            maxPar = spark.read.format('delta').load(table).select("datRefCarga").orderBy(col("datProc").desc()).first()[0]
            log.info(f'Lendo delta table {table} particao {maxPar}')
            df = spark.read.format('delta').load(table).where(f"datRefCarga='{maxPar}'")
        else:
            log.info(f'Lendo delta table {table} particao {partition}')
            df = spark.read.format('delta').load(table).where(f"datRefCarga='{partition}'")
        return df
    
    def write(self, df, table, log, spark, partition):
        
        df_delta = DeltaTable.forPath(spark, table)
        df_delta.delete(f"datRefCarga = '{partition}'")

        log.info(f'Escrevendo delta table {table}')
        df.write.partitionBy('datRefCarga').format("delta").mode("append").save(table)