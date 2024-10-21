from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from kafka import KafkaProducer
from datetime import datetime
import json

class ScrepReclamacoes():
    def run(log):
        try:
            options = Options()
            options.set_capability('se:name', 'Screp Reclamacoes')
            log.info("Iniciando driver")
            driver = webdriver.Remote(options=options,command_executor="http://selenium:4444")
            log.info("Driver inciado")

            driver.get("https://consumidor.gov.br/pages/indicador/geral/abrir")

            wait = WebDriverWait(driver, 60)
            wait.until(EC.element_to_be_clickable((By.ID, 'li_tab_relatos')))

            tab_relatos = driver.find_element(By.ID, "li_tab_relatos")
            tab_relatos.click()

            wait.until(EC.visibility_of_element_located((By.ID, 'contador')))
            contador = driver.find_element(By.ID, 'contador')

            log.info(f"texto contador: {contador.text}")
            data_hora = contador.text.split('desde')[1].strip().replace(',', '')
            data_atu = datetime.strptime(data_hora, '%d/%m/%Y %H:%M:%S')

            try:
                with open('/app/csv/data_hora.bst', encoding='utf8') as file:
                    data_pas = file.readline()
                    data_pas = datetime.strptime(data_pas, '%d/%m/%Y %H:%M:%S')
            except:
                with open('/app/csv/data_hora.bst', "w") as file:
                    file.write(data_hora)
                    data_pas = data_atu

            if data_atu<=data_pas:
                print("Não há atualizações")
                #driver.close()
                exit(0)

            while data_atu>data_pas:
                wait.until(EC.element_to_be_clickable((By.ID, 'btn-mais-resultados')))
                btn_mais_resultados = driver.find_element(By.ID, "btn-mais-resultados")
                btn_mais_resultados.click()

                wait.until(EC.visibility_of_element_located((By.ID, 'contador')))
                contador = driver.find_element(By.ID, 'contador')
                data_hora_l = contador.text.split('desde')[1].strip().replace(',', '')    
                data_atu = datetime.strptime(data_hora_l, '%d/%m/%Y %H:%M:%S')

            with open('/app/csv/data_hora.bst', "w") as file:
                file.write(data_hora)

            resultados = driver.find_element(By.ID, 'resultados')

            nom_empresas = resultados.find_elements(By.CLASS_NAME, 'relatos-nome-empresa')

            empresas = []

            for empresa in nom_empresas:
                empresas.append(empresa.find_element(By.TAG_NAME, 'a').text)

            rel_status = resultados.find_elements(By.TAG_NAME, 'h4')

            lst_status = []

            for status in rel_status:
                lst_status.append(status.text)

            rel_datas = resultados.find_elements(By.CLASS_NAME, 'relatos-data')

            tempo_respostas = []
            datas = []
            cidades = []
            ufs = []
            count = 1

            for data in rel_datas:
                if count % 2 == 0:
                    tempo_respostas.append(data.text.replace('(','').replace(')',''))   
                else:
                    datas.append(data.text.split(',')[0])
                    cidades.append(data.text.split(',')[1].split('-')[0].strip())
                    ufs.append(data.text.split(',')[1].split('-')[1].strip())
                count=count+1

            lst_relatos = resultados.find_elements(By.XPATH, "//div[3]/p")
            lst_respostas = resultados.find_elements(By.XPATH, "//div[4]/p")
            lst_notas_comentarios = resultados.find_elements(By.XPATH, "//div[5]/p")

            relatos = []
            respostas = []
            notas = []
            comentarios = []

            for relato in lst_relatos:
                relatos.append(relato.text)

            for resposta in lst_respostas:
                respostas.append(resposta.text)

            count = 1
            for nota in lst_notas_comentarios:
               if count % 2 == 0:
                   comentarios.append(nota.text)
               else:
                   notas.append(nota.text)
               count=count+1

            relatos = list(zip(empresas,lst_status,tempo_respostas, datas, cidades, ufs, relatos, respostas, notas, comentarios))            

            keys = ['nomeEmpresa' ,'status' ,'tempoResposta' ,'dataOcorrido' ,'Cidade' ,'UF' ,'Relato' ,'Resposta' ,'Nota' ,'Comentario']
            lst_relatos = []
            for item in relatos:
               key_value_pairs = zip(keys, item)
               my_dict = dict(key_value_pairs)
               lst_relatos.append(my_dict)

            producer = KafkaProducer(bootstrap_servers=["kafka:9092"])

            producer.send("reclamacoes", str(lst_relatos).encode())

            log.info("enviado para Kafka")
        except Exception as error:
            log.info("An exception occurred:", error)
        finally:
            driver.close()