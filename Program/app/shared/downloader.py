from . import utils
import os
import requests
import csv
from io import BytesIO, StringIO
import geopandas as gpd
import pandas as pd

def readOnlineCsvFile(csvfileLink):
    try:
        print("Inciando o download, aguarde...")

        #directory = os.path.dirname(__file__)
        #file_path = os.path.join(directory, "certificate.crt")
        file_path = False
        response = requests.get(csvfileLink, verify=file_path)
        csv_data = response.content.decode('utf-8')
        # csv_data = response.text
        csv_reader = csv.DictReader(csv_data.splitlines())
        return csv_reader

        print("Download Finalizado!")
    except Exception as e:
        print(e)    
        return 'ERRO'    

def readOnlineCsvFileByGpd(csvfileLink):
    try:
        print("Inciando o download, aguarde...")
        file_path = False
        response = requests.get(csvfileLink, verify=file_path)
        csv_data = response.content.decode('utf-8')

        csv_io = StringIO(csv_data)
        result = pd.read_csv(csv_io, encoding="utf-8", sep=",", dtype=None, on_bad_lines='warn')
        # csv_reader = csv.DictReader(csv_data.splitlines())
        # result = gpd.GeoDataFrame.from_records(csv_reader)
        return result

        print("Download Finalizado!")
    except Exception as e:
        print(e)    
        return 'ERRO'  

def saveDownloadCsvFileOnline(csvfileLink, csvfile):
    try:
        print("Inciando o download, aguarde...")

        directory = utils.getDownloadPath()
        file_path = os.path.join(directory, csvfile)
        response = requests.get(csvfileLink, verify=False)

        with open(file_path, "wb") as file:
            file.write(response.content)

        print("Download Finalizado!")
    except Exception as e:
        print(e)    
        return 'ERRO'    

def readZipOnlineFile(file_url):
    try:
        response = requests.get(file_url, verify=False)
        zip_content = BytesIO(response.content)
        result = gpd.read_file(zip_content, encoding='utf-8')
        print("Download Finalizado")
        return result
    except Exception as e:
        print(e)    
        return 'ERRO'   

def readZipLocalFile(fileName):
    try:
        directory = utils.getDataPath()
        file_path = os.path.join(directory, fileName)
        #zip_content = BytesIO(utils.readFile(file_path))
        result = gpd.read_file(file_path, encoding='utf-8')
        print("Leitura Finalizada")
        return result
    except Exception as e:
        print(e)    
        return 'ERRO'    
