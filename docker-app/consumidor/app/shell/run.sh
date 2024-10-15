export MY_DIR=$(cd $(dirname "${0}"); pwd)
export base_path="$(cd "$MY_DIR/.."; pwd)"
export csv_path=${base_path}/csv

if [[ ${1} == "stream_bronze" ]]; then
    /spark/bin/spark-submit \
        --properties-file ${base_path}/conf/spark-defaults_local.conf \
        ${base_path}/main_spark.py ${1} $csv_path  
elif [[ ${1} == "download" ]]; then
    python3 ${base_path}/main.py ${1} ${2} $csv_path
elif [[ ${1} == "screp" ]]; then
    python3 ${base_path}/main.py ${1} ${2} $csv_path     
else
    /spark/bin/spark-submit \
        --properties-file ${base_path}/conf/spark-defaults_master.conf \
        ${base_path}/main_spark.py ${1} ${2} "hdfs://namenode:9000/data/consumidor/landing"
fi        
