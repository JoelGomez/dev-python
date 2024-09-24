import time
import csv
import logging
from dotenv import dotenv_values
from concurrent.futures import ThreadPoolExecutor
from suds.client import Client


config = dotenv_values("../.env")
usr = config['USR']
pws = config['PWS']
rfc_emisor = config['RFC_EMISOR']
ws_url = config['WS_URL']
client = Client(ws_url)
count = 0
time_start = time.perf_counter()
data_uuid = []


# logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')
logger = logging.getLogger()
logging.basicConfig(filename='consulta_estatus.log', level=logging.DEBUG, format='%(threadName)s: %(message)s')


def check_status(uuid, total, rfc_receptor):
    global count
    response = client.service.ConsultaEstatus(usr, pws, rfc_emisor, rfc_receptor, total, uuid)
    canceled = response.CancelacionExitosa

    print(f'Checking status for {uuid}...')

    if canceled:
        logging.info(f'{uuid} was canceled')
        count += 1       
    else:
        logging.info(f'{uuid} was not canceled')    
    

if __name__ == '__main__':
    with open('../assets/consulta_estatus.csv', 'r') as data_file:
        data = csv.reader(data_file)
        for row in data:      
            data_uuid.append([row[0], row[1], row[2]])

        data_file.close()
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(check_status, [uuid[0] for uuid in data_uuid], [uuid[1] for uuid in data_uuid], [uuid[2] for uuid in data_uuid])

    time_finish = time.perf_counter()
    print("\033[0;32m" + '#' * 69 + "\033[0;m")
    print(f'{count} rows were canceled and {len(data_uuid)-count} rows were not canceled')
    # print(f'Finished in {round((time_finish-time_start)/60, 2)} minutes(s)')

    print(f'{(time_finish-time_start)//60} minutes and {round((time_finish-time_start)%60,2)} seconds')

