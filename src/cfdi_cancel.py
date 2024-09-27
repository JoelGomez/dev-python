from csv import reader
from datetime import datetime
from suds.client import Client
from dotenv import dotenv_values
import concurrent.futures

config = dotenv_values("../.env")


url_ws_proveedor = config['URL_WS_FERAZ']
compania = config['COMPANIA']
usr = config['USR_FERAZ']
pws = config['PWS_FERAZ']


counter = 0
cliente = Client(url_ws_proveedor)
data = []


#entry data
tipo_cancelacion = '01'
file_to_process = '../assets/file_list_to_cancel.csv'

start = datetime.now()
print(f'Iniciando proceso de cancelación de CFDIs: {start}')

def cancel_cfdi(compania, uuid_cancelable, tipo_cancelacion, uuid_relacionado, usr, pws):
     global counter
     print(f'Cancelando CFDI {uuid_cancelable}...')
     response = cliente.service.cancelaCFDI(compania, uuid_cancelable, tipo_cancelacion, uuid_relacionado, usr, pws)
     print(uuid_cancelable, response)
     counter += 1



if __name__ == '__main__':
    with open(file_to_process, 'r') as file:
        row_file = reader(file)

        for row in row_file:
            data.append(row)
        
        print(len(data), 'CFDIs to cancel')    
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(cancel_cfdi, [compania for row in data], [row[0] for row in data], [tipo_cancelacion for row in data], [row[1] for row in data], [usr for row in data], [pws for row in data])

    finish = datetime.now() - start
    print(f'Proceso finalizado: {finish}')
    print(f'CFDIs cancelados {counter}')
    
        