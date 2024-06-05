import platform
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def getLogsPath(log_name_file):
    current_directory = os.path.dirname(__file__)
    relative_path = f"../../logs"
    directory = os.path.abspath(os.path.join(current_directory, relative_path))
    if not os.path.exists(directory):
       os.makedirs(directory) 
    relative_path = f"../../logs/{log_name_file}"
    file_path = os.path.abspath(os.path.join(current_directory, relative_path))
    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            file.write("")
    return file_path

def getEnvPath():
    current_directory = os.path.dirname(__file__)
    relative_path = "../../.env"
    directory = os.path.abspath(os.path.join(current_directory, relative_path))
    return directory

def getDataPath(file=None):
    current_directory = os.path.dirname(__file__)
    relative_path = f"../data/{file}" if file != None else "../data/"
    directory = os.path.abspath(os.path.join(current_directory, relative_path))
    return directory

def getDownloadPath():
    current_directory = os.path.dirname(__file__)
    relative_path = "../data/10min-data/"
    directory = os.path.abspath(os.path.join(current_directory, relative_path))
    return directory

def getDateTimeFromLastFileDownload():
    directory = getDownloadPath()
    files = os.listdir(directory)
    files = [os.path.join(directory, file) for file in files if os.path.isfile(os.path.join(directory, file))] #Get file paths
    files.sort(key=os.path.getmtime) #Modification time sort

    # Get the last file
    if files:
        base, extension = os.path.splitext(files[-1].split("x_")[1])
        date_splited = base.split("_")
        date = date_splited[0]
        hours_min = date_splited[1]
        return datetime.strptime(f"{date} {hours_min}", "%Y%m%d %H%M")
    else:
        #if directory is empty
        return datetime.strptime('2024-02-03 23:50', '%Y-%m-%d %H:%M')

def getDataFrameFromCSVFile(file_path, encoding="utf-8", sep=",", dtype_options=None):
    try:
        content = pd.read_csv(file_path, encoding=encoding, sep=sep, dtype=dtype_options, on_bad_lines='warn')
        return content
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def readFile(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def writeFile(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
    except Exception as e:
        return f"An error occurred: {str(e)}"

def go_through_list(array_list, list_limit, index):
    if index != None:
        result = array_list[((list_limit * index) - list_limit):(list_limit * index)]
    else:
        result = array_list[(len(array_list) - list_limit): len(array_list)]
    return result

def replaceAll(value, search, new_value):
    return value.replace(search, new_value)

def numberAdjust(numero):
    numero = ''.join(filter(str.isdigit, str(numero)))
    tamanho = len(numero)

    if tamanho == 12:
        numero = '55' + numero[1:3] + numero[4:]

    if tamanho == 11:
        numero = '55' + numero[:2] + numero[3:]

    if tamanho == 10:
        numero = '55' + numero

    return numero

def getDateHourNasa(date_hour_str):
    data_str, minutos_segundos_str = date_hour_str.split(',')

    data_formatada = datetime.strptime(data_str, "%Y-%m-%d")
    minutos_segundos = int(minutos_segundos_str)
    tempo_formatado = timedelta(minutes=minutos_segundos // 60, seconds=minutos_segundos % 60)
    data_hora_formatada = data_formatada + tempo_formatado
    
    return data_hora_formatada

def dateHourFormat(data_hora):
    data_hora_str = pd.Timestamp(data_hora).strftime('%Y-%m-%d %H:%M:%S')
    dt = datetime.strptime(data_hora_str, "%Y-%m-%d %H:%M:%S")
    novo_formato = dt.strftime("%Y/%m/%d %H:%M:%S")
    return novo_formato

def formatData(value):
    splitted = value.split(',')
    corrected_months = []
    combined_month = ''

    for month in splitted:
        if month.startswith('"') and not month.endswith('"'):
            combined_month = month
        elif month.endswith('"') and combined_month:
            combined_month += f',{month}'
            corrected_months.append(combined_month)
            combined_month = ''
        elif combined_month:
            combined_month += f',{month}'
        else:
            corrected_months.append(month)


    print(corrected_months)