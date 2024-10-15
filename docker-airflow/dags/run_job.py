import pendulum 
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.operators.bash import BashOperator

if Variable.get('dat_ref_carga') == "":
    dat_ref_carga = datetime.now() - timedelta(days=60)
    dat_ref_carga = dat_ref_carga.strftime("%Y-%m")
else:    
    dat_ref_carga = Variable.get('dat_ref_carga')

path_project = Variable.get('path_project')

default_args = {
    'email': ['lucas.san20@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': pendulum.datetime(2024, 7, 1)
}

COMMON_DOCKER_OP = dict(
  image="consumidor-app:latest",
  docker_url="unix:/var/run/docker.sock",
  auto_remove=True,
  mount_tmp_dir=False,
  network_mode='hadoop-spark',
)

@dag(schedule='@monthly', catchup=False, default_args=default_args)
def run_jobs():    
    run_download = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_download',
        entrypoint=f"sh /app/shell/run.sh download {dat_ref_carga}",
        container_name='consumidor-app-download',
        mounts=[
            Mount(source=f"{path_project}/csv", target="/app/csv", type="bind"),                
        ],        
        default_args={
            'retries': 20,
            'retry_delay': timedelta(seconds=5),
        }
    )

    put_hdfs = BashOperator(
      task_id="put_hdfs",
      bash_command=f"""docker exec namenode sh -c 'hdfs dfs -put -f /csv/basecompleta{dat_ref_carga}*.csv /data/consumidor/landing'""",      
    )

    run_bronze = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_bronze',
        entrypoint=f"sh /app/shell/run.sh bronze {dat_ref_carga}",
        container_name='consumidor-app-bronze',
    )

    run_silver = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_silver',
        entrypoint=f"sh /app/shell/run.sh silver {dat_ref_carga}",
        container_name='consumidor-app-silver',
    )    

    run_grupo_problema = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_grupo_problema',
        entrypoint=f"sh /app/shell/run.sh grupo_problema {dat_ref_carga}",
        container_name='consumidor-app-grupo-problema',
    )
    
    run_top_ten = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_top_ten',
        entrypoint=f"sh /app/shell/run.sh top_ten {dat_ref_carga}",
        container_name='consumidor-app-top-ten',
    )

    run_avaliacao = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_avaliacao',
        entrypoint=f"sh /app/shell/run.sh avaliacao {dat_ref_carga}",
        container_name='consumidor-app-avaliacao',
    )     

    run_resposta = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_resposta',
        entrypoint=f"sh /app/shell/run.sh resposta {dat_ref_carga}",
        container_name='consumidor-app-resposta',
    )
    
    run_uf = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_uf',
        entrypoint=f"sh /app/shell/run.sh uf {dat_ref_carga}",
        container_name='consumidor-app-uf',
    )     

    (run_download >> put_hdfs >> run_bronze >> run_silver >> \
        [run_grupo_problema, run_top_ten, run_avaliacao, run_resposta, run_uf]) 

run_jobs()