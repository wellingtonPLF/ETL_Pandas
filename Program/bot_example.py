from geoalchemy2 import Geometry
from shapely.geometry import MultiPolygon
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import insert
from datetime import datetime, timedelta
from sqlalchemy.sql import text
import time
import pandas as pd
import traceback
import logging

import app.config.mapping as mapping
import app.config.connection as connection
import app.shared.downloader as downloader
import app.shared.utils as utils

start_time = time.time()

logging.basicConfig(filename=utils.getLogsPath(log_name_file='bot_example.log'), format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO, encoding='utf-8')
logging.info(f'# Inicio do Processamento - {str(datetime.today())}')

url = "https://an-url-that-you-can-retrive-data-like-json-csv-xml-or-others.com"

try:
    engine =connection.getConnectionSqlAlchemyGetEngine()
    logging.info('CONNECTION HAS BEEN MADED!')

    result_data = downloader.readZipOnlineFile(url) // if you wanna use local data, just see downloader.py
    logging.info('DATA HAS BEEN READ!')

# ************************************************************************************************************************************
    result_data.set_crs('EPSG:4674', allow_override=True)

    result_data = result_data.rename(columns=lambda x: x.lower())
    result_data = result_data.loc[result_data['column_2'] == 'PB']
    result_data = result_data.rename(columns={'geometry': 'column_4'})
    result_data['column_4'] = result_data['column_4'].apply(lambda x: MultiPolygon([x]) if x.geom_type=='Polygon' else x)

    registry_array = []
    limite_data = datetime.today() - timedelta(days=600000)
    result_data['column_3'] = pd.to_datetime(result_data['column_3'])
    filtro = result_data['column_3'] >= limite_data
    dados_60_dias = result_data[filtro]

    for index, row in dados_60_dias.iterrows():     

            _registry = {
                'integerColumn ':row['column_1'],
                'stringColumn':row['column_2'],
                'dateColumn': row['column_3'],
                'geomColumn': row['column_4'],
                'floatColumn': row['column_5'],
                'textoColumn': row['column_6'],
                'boolColumn': row['column_7'],
                'date_timeColumn': row['column_8']
            }
            registry_array.append(_registry)
    
    logging.info(f"Number of Registrys: {len(registry_array)}")

    if(len(registry_array) > 0):
        
        Session = sessionmaker(bind=engine)
        session = Session()
        insert_stmt = insert(mapping.MyTable).values(registry_array)     
        on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
        session.execute(on_conflict_stmt)
        session.commit()     
        print("inserts finalizados")        
        logging.info('INSERTS ARE DONE!')
    else:
        print('nenhum registro adicionado.')
        logging.info('NO REGISTRY WAS FOUND!')
    
# ************************************************************************************************************************************
    
except Exception as e:
    print("Error: ", e)
    logging.error(f'Um erro ocorreu: {str(e)}')
    logging.error(f'Um erro ocorreu: {str(traceback.format_exc())}')

end_time = time.time()
total_time = end_time - start_time
print('Tempo de execução em segundo: {} '.format(str(total_time)))
print('PROCESSAMENTO FINALIZADO!')
logging.info(f'# Tempo de execução em segundo: {str(total_time)}')
logging.info('==========================================================================')
