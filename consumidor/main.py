from src.download_csv import DownloadCsv

import sys
import logging

logging.basicConfig(
    level=logging.INFO
    ,format='%(asctime)s: CONSUMIDOR: LOG INFO: %(message)s'
)

class Main():
    
    log = logging.getLogger()
    processamento=sys.argv[1]

    log.info(f"Processamento {processamento}")
    parm2=sys.argv[2]
    log.info(f"Data processamento {parm2}")

    DownloadCsv.run(log, parm2, sys.argv[3])  