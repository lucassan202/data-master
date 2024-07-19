import pendulum 

from airflow.decorators import dag, task
dat_ref_carga = '2023-08'
path_project = '/home/besgam/Projetos/data-master/consumidor/shell/run.sh'

@dag(schedule='@monthly', start_date=pendulum.datetime(2024, 7, 1), catchup=False)
def run_jobs():
    @task.bash
    def run_silver() -> str:
        return f"{path_project} silver {dat_ref_carga}"

    run_silver = run_silver()

    @task.bash
    def run_grupo_problema() -> str:
        return f"{path_project} grupo_problema {dat_ref_carga}"

    run_grupo_problema = run_grupo_problema()

    @task.bash
    def run_top_ten() -> str:
        return f"{path_project} top_ten {dat_ref_carga}"

    run_top_ten = run_top_ten()

    @task.bash
    def run_avaliacao() -> str:
        return f"{path_project} avaliacao {dat_ref_carga}"

    run_avaliacao = run_avaliacao()

    @task.bash
    def run_resposta() -> str:
        return f"{path_project} resposta {dat_ref_carga}"

    run_resposta = run_resposta()

    @task.bash
    def run_uf() -> str:
        return f"{path_project} uf {dat_ref_carga}"

    run_uf = run_uf()   

    (run_silver >> [run_grupo_problema, run_top_ten, run_avaliacao, run_resposta, run_uf])

run_jobs()