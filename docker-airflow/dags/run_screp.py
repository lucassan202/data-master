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
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
    'start_date': pendulum.datetime(2024, 7, 1)
}

COMMON_DOCKER_OP = dict(
  image="consumidor-app:latest",
  docker_url="unix:/var/run/docker.sock",
  auto_remove=True,
  mount_tmp_dir=False,
  network_mode='hadoop-spark',
)

@dag(schedule='@hourly', catchup=False, default_args=default_args)
def run_screp():    
    run_scre = DockerOperator(
        **COMMON_DOCKER_OP,
        task_id='run_screp',
        entrypoint=f"sh /app/shell/run.sh screp {dat_ref_carga}",
        container_name='consumidor-app-screp',
        mounts=[
            Mount(source=f"{path_project}/csv", target="/app/csv", type="bind"),                
        ],
    )

    (run_scre) 

run_screp()