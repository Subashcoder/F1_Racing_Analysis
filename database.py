import pymysql
from dotenv import load_dotenv
import os
import requests
from datetime import datetime
from dateutil import parser

load_dotenv()

MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

conn = pymysql.connect(
    host = 'localhost',
    user = 'root',
    passwd=MYSQL_PASSWORD,
    db = 'F1_Racing',
    
)

session_url = 'sessions?'
driver_url =  'drivers?'
meeting_url = 'meetings?'
base_url = 'https://api.openf1.org/v1/'

def session_data(base_url,session,year):
    url = f'{base_url}{session}year={year}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        sesssion_keys = [dic['session_key'] for dic in data if dic['session_type'] == 'Race' and dic['session_name'] == 'Race' ]
        return sesssion_keys,data
    else:
        print('API error')
        
def API_CALL(base_url, endpoint, year):
    url = f'{base_url}{endpoint}year={year}'
    response = requests.get(url=url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print('API Call error!!!')
        

def driver_data(base_url, driver_url, session_key):
    url = f'{base_url}{driver_url}'
    response = requests.get(url=url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print('API Error!!')
        




# CREATING Driver information table TABLE
sql_table1 = '''CREATE TABLE IF NOT EXISTS Driver_info (
    driver_number INT NOT NULL ,
    broadcast_name VARCHAR(255),
    country_code VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    full_name VARCHAR(255),
    headshot_url VARCHAR(255),
    meeting_key INT,
    name_acronym CHAR(3),
    session_key INT,
    team_color VARCHAR(255),
    team_name VARCHAR(255)    
    )'''
    
sql_table2 = '''CREATE TABLE IF NOT EXISTS f1_meeting(
    MeetingKey INT NOT NULL,
    MeetingName VARCHAR(255),
    CountryName VARCHAR(255),
    StartDate DATETIME,
    CircuitName VARCHAR(255),
    Location Varchar(255),
    Year INT,
    PRIMARY KEY (MeetingKey)
    )'''
    
sql_table3 = '''
    CREATE TABLE IF NOT EXISTS f1_sessions (
        SessionKey INT NOT NULL,
        SessionName VARCHAR(255),
        SessionType VARCHAR(255),
        MeetingKey INT NOT NULL,
        StartDate DATETIME,
        EndDate DATETIME,
        PRIMARY KEY (SessionKey),
        FOREIGN KEY (MeetingKey) REFERENCES f1_meeting(MeetingKey) 
    )
'''

sql2 = 'SHOW TABLES'

insert_sql1 = '''
INSERT INTO Driver_info values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''

insert_sql2 = '''
    INSERT INTO f1_meeting values(%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
    MeetingName = VALUES(MeetingName),
    CountryName = VALUES(CountryName),
    StartDate = VALUES(StartDate),
    CircuitName = VALUES(CircuitName),
    Location = VALUES(Location),
    Year = VALUES(Year);
'''

insert_sql3 = '''
    INSERT INTO f1_sessions values (%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
    SessionName = VALUES(SessionName),
    SessionType = values(SessionType),
    MeetingKey = values(MeetingKey),
    StartDate =  values(StartDate),
    EndDate = Values(EndDate)  
'''

mycursor = conn.cursor()
mycursor.execute(sql_table3)

def data_withdatetime():
    meeting_data = API_CALL(base_url, session_url, 2024)
    for data in meeting_data:
        startdate = parser.parse(data['date_start']).strftime('%Y-%m-%d %H:%M:%S')
        enddate = parser.parse(data['date_end']).strftime('%Y-%m-%d %H:%M:%S')
        mycursor.execute(insert_sql3, (data['session_key'], data['session_name'],data['session_type'], data['meeting_key'],
                        startdate, enddate))
    conn.commit()
    
   
ddriverinfo = driver_data(base_url, driver_url,2023)
for data in ddriverinfo:
    mycursor.execute(insert_sql1, (data['driver_number'], data['broadcast_name'], data['country_code'], data['first_name'], data['last_name'],
                                   data['full_name'], data['headshot_url'], data['meeting_key'], data['name_acronym'], data['session_key'],
                                   data['team_colour'], data['team_name']))
    print('added successfully')
conn.commit()


    
     