#from src.stream_bronze import StreamBronze
from src.silver import Silver
from src.problema_gold import GrupoProblema
from src.reclamacao_gold import ReclamacaoTopTen
from src.avaliacao_gold import MediaAvaliacao
from src.uf_gold import UfProblema
from src.resposta_gold import MediaResposta
from src.bronze import Bronze

import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO
    ,format='%(asctime)s: CONSUMIDOR: DEBUG INFO: %(message)s'
)

class Main():
    
    log = logging.getLogger()
    processamento=sys.argv[1]

    log.info(f"Processamento {processamento}")
    if processamento == 'stream_bronze':
        parm2 = sys.argv[2]
        log.info(f"Path read stream csv {parm2}")
    else:
        parm2=sys.argv[2]
        log.info(f"Data processamento {parm2}")

    
    spark = SparkSession.builder.appName(processamento) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport().getOrCreate()          

    def main(spark, processamento, log, parm2):    
        
        if processamento == 'stream_bronze':
            #StreamBronze.run(spark, log, parm2)
            Bronze.run(spark, log, parm2, sys.argv[3])
        elif processamento == 'silver':
            Silver.run(spark, log, parm2)
        elif processamento == 'grupo_problema':
            GrupoProblema.run(spark, log, parm2)
        elif processamento == 'top_ten':
            ReclamacaoTopTen.run(spark, log, parm2)
        elif processamento == 'avaliacao':
            MediaAvaliacao.run(spark, log, parm2) 
        elif processamento == 'resposta':
            MediaResposta.run(spark, log, parm2) 
        elif processamento == 'uf':
            UfProblema.run(spark, log, parm2)
        elif processamento == 'bronze':
            Bronze.run(spark, log, parm2, sys.argv[3])              
    
    main(spark, processamento, log, parm2)