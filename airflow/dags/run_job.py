import pendulum 

from airflow.decorators import dag, task
dat_ref_carga = "2023-09"

@dag(schedule='@monthly', start_date=pendulum.datetime(2024, 7, 1), catchup=False)
def run_jobs():
    @task.bash
    def run_silver() -> str:
        return f"/home/besgam/Projetos/data-master/consumidor/shell/run.sh silver {dat_ref_carga}"

    run_silver = run_silver()

    @task.bash
    def run_grupo_problema() -> str:
        return f"/home/besgam/Projetos/data-master/consumidor/shell/run.sh grupo_problema {dat_ref_carga}"

    run_grupo_problema = run_grupo_problema()

    @task.bash
    def run_top_ten() -> str:
        return f"/home/besgam/Projetos/data-master/consumidor/shell/run.sh top_ten {dat_ref_carga}"

    run_top_ten = run_top_ten()

    run_silver >> [run_grupo_problema, run_top_ten]    

run_jobs()