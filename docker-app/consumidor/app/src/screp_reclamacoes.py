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
            log.info("Iniciando driver Selenium")
            driver = webdriver.Remote(options=options,command_executor="http://selenium:4444")
            log.info("Driver inciado")

            driver.get("https://consumidor.gov.br/pages/indicador/relatos/abrir")

            wait = WebDriverWait(driver, 60)
            # wait.until(EC.element_to_be_clickable((By.ID, 'li_tab_relatos')))

            # tab_relatos = driver.find_element(By.ID, "li_tab_relatos")
            # tab_relatos.click()

            wait.until(EC.visibility_of_element_located((By.ID, 'contador')))
            contador = driver.find_element(By.ID, 'contador')
            
            data_hora = contador.text.split('desde')[1].strip().replace(',', '')
            log.info(f"Data atual disponível: {data_hora}")
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
                log.info(f"Data última atualização bst: {data_pas}")
                wait.until(EC.element_to_be_clickable((By.ID, 'btn-mais-resultados')))
                btn_mais_resultados = driver.find_element(By.ID, "btn-mais-resultados")
                btn_mais_resultados.click()

                wait.until(EC.visibility_of_element_located((By.ID, 'contador')))
                contador = driver.find_element(By.ID, 'contador')
                data_hora_l = contador.text.split('desde')[1].strip().replace(',', '')    
                data_atu = datetime.strptime(data_hora_l, '%d/%m/%Y %H:%M:%S')
                log.info(f"Data atual disponível: {data_atu}")

            with open('/app/csv/data_hora.bst', "w") as file:
                file.write(data_hora)

            resultados = driver.find_element(By.ID, 'resultados')

            nom_empresas = resultados.find_elements(By.CLASS_NAME, 'relatos-nome-empresa')

            empresas = []

            log.info("Buscando nome empresas by CLASS_NME relatos-nome-empresa")
            for empresa in nom_empresas:
                empresas.append(empresa.find_element(By.TAG_NAME, 'a').text)

            rel_status = resultados.find_elements(By.TAG_NAME, 'h4')

            lst_status = []

            log.info("Buscando status reclamações by TAG_NAME h4")

            for status in rel_status:
                lst_status.append(status.text)                    

            tempo_respostas = []
            datas = []
            cidades = []
            ufs = []            

            lst_ocorrido = resultados.find_elements(By.XPATH, "//div[3]/span")

            log.info("Buscando data, cidade e UF reclamações by XPATH //div[3]/span")

            for data in lst_ocorrido:
                datas.append(data.text.split(',')[0])
                cidades.append(data.text.split(',')[1].split('-')[0].strip())
                ufs.append(data.text.split(',')[1].split('-')[1].strip())

            rel_datas = resultados.find_elements(By.XPATH, "//div[4]/span")

            log.info("Buscando tempo resposta by XPATH //div[4]/span")

            for data in rel_datas:
                tempo_respostas.append(data.text.replace('(','').replace(')',''))

            lst_relatos = resultados.find_elements(By.XPATH, "//div[3]/p")
            lst_respostas = resultados.find_elements(By.XPATH, "//div[4]/p")
            lst_notas_comentarios = resultados.find_elements(By.XPATH, "//div[5]/p")

            relatos = []
            respostas = []
            notas = []
            comentarios = []

            log.info("Buscando relatos do consumidor by XPATH //div[3]/p")

            for relato in lst_relatos:
                relatos.append(relato.text)

            log.info("Buscando respostas da empresa by XPATH //div[4]/p")

            for resposta in lst_respostas:
                respostas.append(resposta.text)

            log.info("Buscando notas e comentários do consumidor by XPATH //div[5]/p")
            count = 1
            for nota in lst_notas_comentarios:
               if count % 2 == 0:
                   comentarios.append(nota.text)
               else:
                   notas.append(nota.text)
               count=count+1

            log.info("Zip relatos")
            relatos = list(zip(empresas,lst_status,tempo_respostas, datas, cidades, ufs, relatos, respostas, notas, comentarios))            

            keys = ['nomeEmpresa' ,'status' ,'tempoResposta' ,'dataOcorrido' ,'Cidade' ,'UF' ,'Relato' ,'Resposta' ,'Nota' ,'Comentario']
            lst_relatos = []

            log.info("Serializando list to JSON")
            for item in relatos:
               key_value_pairs = zip(keys, item)
               my_dict = dict(key_value_pairs)
               lst_relatos.append(my_dict)

            producer = KafkaProducer(bootstrap_servers=["kafka:9092"])

            log.info("Enviando para tópico Kafka reclamacoes")
            #print(lst_relatos)

            producer.send("reclamacoes", str(lst_relatos).encode())

            log.info("enviado para Kafka")
        except Exception as error:
            log.info("An exception occurred:", error)
        finally:
            driver.close()