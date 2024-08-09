from delta.tables import DeltaTable

class Data:
    def read(self, table, partition, spark, log):
        log.info(f'Lendo delta table {table} particao {partition}')
        return spark.read.format('delta').load(table).where(f"datRefCarga='{partition}'")
    
    def write(self, df, table, log, spark, partition):
        
        df_delta = DeltaTable.forPath(spark, table)
        df_delta.delete(f"datRefCarga = '{partition}'")

        log.info(f'Escrevendo delta table {table}')
        df.write.partitionBy('datRefCarga').format("delta").mode("append").save(table)