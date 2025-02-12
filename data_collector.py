import requests
import os
import json
from pprint import pprint
import pandas as pd
import os


base_url = 'https://api.openf1.org/v1/'
car_data = 'https://api.openf1.org/v1/car_data?driver_number=2&session_key=9159'
driver_data =  'drivers'
lap_data = 'https://api.openf1.org/v1/laps?session_key=7763&driver_number=2'
session_data = 'https://api.openf1.org/v1/sessions?year=2023'

response_drive = requests.get(url=f'{base_url}{driver_data}?session_key=7953')
response_cardata = requests.get(url=car_data)
response_lap = requests.get(url=lap_data)

data = response_drive.json()
print(data)

def get_data(url):
    response = requests.get(url=url)
    if response.status_code == 200:
        print('Driver Info:')
        data = response.json()
        print(len(data))
        sesssion_keys = [dic['session_key'] for dic in data if dic['session_type'] == 'Race' and dic['session_name'] == 'Race' ]
        print(sesssion_keys)
        # pprint(data[1:5])
        print("Starting")
        print(data[1].keys())
        pprint(data[1])
        if os.path.exists('data.csv'):
            print("Pass file exists.")
        else:
            df = pd.DataFrame(data=data)
            df.to_csv('data.csv',header=True)
            print("CSV save successfully.")
            

    
