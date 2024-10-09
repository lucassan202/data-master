import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup

class DownloadCsv():

    def run(log, datRefCarga, pathCsv):
        def download_csv(url, destinationPath):
            response = requests.get(url)
            response.raise_for_status()

            with open(destinationPath, 'wb') as file:
                file.write(response.content)

        html = urlopen("https://dados.mj.gov.br/dataset/reclamacoes-do-consumidor-gov-br")
        bs = BeautifulSoup(html, 'html.parser')

        linhas = bs.find_all('a', {'class':'resource-url-analytics'})
        
        links = []

        for link in linhas:
            links.append(link.get('href'))

        count = 0
        for url in links:
            if datRefCarga in url:
                count=count+1
                log.info("Iniciando download csv data {}".format(datRefCarga))
                download_csv(url, "{}/{}v".format(pathCsv, url[url.rfind("/")+1: -1]))
                log.info("Download concluído")
        
        if count == 0:
            log.info("Data {} não disponível para download".format(datRefCarga))