import pendulum 

from airflow.decorators import dag, task

path_project = '/home/besgam/Projetos/data-master/consumidor/shell/run.sh'

@dag(schedule='@monthly', start_date=pendulum.datetime(2024, 7, 1), catchup=False)
def run_stream():
    @task.bash
    def run() -> str:
        return f"{path_project} stream_bronze"

    run()

run_stream()