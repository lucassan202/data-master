class Data:
    def read(self, table, partition, spark, log):
        log.info(f'Lendo delta table {table} particao {partition}')
        return spark.read.format('delta').load(table).where(f"datRefCarga='{partition}'")
    
    def write(self, df, table, log):
        log.info(f'Escrevendo delta table {table}')
        df.write.partitionBy('datRefCarga').format("delta").mode("append").save(table)