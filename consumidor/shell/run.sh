export MY_DIR=$(cd $(dirname "${0}"); pwd)
export base_path="$(cd "$MY_DIR/.."; pwd)"
export csv_path=${base_path}/csv

if [[ ${1} == "stream_bronze" ]]; then
    spark-submit \
        --properties-file ${base_path}/conf/spark-defaults_local.conf \
        ${base_path}/main.py ${1} $csv_path
else
    spark-submit \
        --properties-file ${base_path}/conf/spark-defaults_master.conf \
        ${base_path}/main.py ${1} ${2}
fi        