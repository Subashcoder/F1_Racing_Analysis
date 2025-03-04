import requests
import pandas as pd
import snowflake.connector
from datetime import datetime
from dateutil import parser
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"

load_dotenv(env_path)

conn = snowflake.connector.connect(
    user = os.getenv('user'),
    password = os.getenv('password'),
    account = os.getenv('account'),
    warehouse = 'dbt_wa',
    database = 'f1_Racing',
    schema = 'f1_Racing_raw'
)
cursor = conn.cursor()

print('COnnect successfully')

cursor = conn.cursor()

cursor.execute('USE ROLE dbt_role')

cursor.execute('use schema f1_Racing_raw')

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
    )'''
    
sql_table4 = '''
CREATE TABLE if not exists f1_RaceControl (
SessionKey INT,
DriverNumber INT,
Category VARCHAR(255),
IncidentTime datetime,
Flag varchar(255),
LapNumber INT,
Message VARCHAR(),
Scope VARCHAR(255),
foreign key (SessionKey) references f1_sessions(SessionKey)
)
'''

sql_table5 = '''
    CREATE TABLE IF NOT EXISTS Weather (
SessionKey INT,
CurrentDate Datetime,
Humidity FLOAT,
Pressure FLOAT,
Rainfall INT,
TrackTemperature FLOAT,
WindDirection FLOAT,
WindSpeed FLOAT,
FOREIGN KEY (SessionKey) references f1_sessions(SessionKey)
)
'''

sql_table6 = '''
CREATE TABLE IF NOT EXISTS f1_Laps (
DriverNumber INT,
SessionKey INT,
LapNumber INT,
LapDuration FLoat,
DurationSector1 FLOAT,
DurationSector2 FLOAT,
DurationSector3 FLOAT,
Speed FLOAT,
FOREIGN KEY (SessionKey) references f1_sessions(SessionKey)
)
'''

insert_sql1 = '''
INSERT INTO Driver_info values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''

insert_sql2 = '''
    INSERT INTO f1_meeting values(%s,%s,%s,%s,%s,%s,%s);
'''

insert_sql3 = '''
    INSERT INTO f1_sessions values (%s,%s,%s,%s,%s,%s)

'''

insert_raceControl = '''
INSERT INTO f1_RaceControl values (%s, %s, %s,%s, %s, %s, %s, %s)
'''

insert_laprecords = 'INSERT INTO f1_Laps Values (%s, %s, %s,%s, %s, %s, %s, %s)'
    
cursor.execute(sql_table1)
cursor.execute(sql_table2)
cursor.execute(sql_table3)
cursor.execute(sql_table4)
cursor.execute(sql_table5)
cursor.execute(sql_table6)

class RawDataIngestion:
    def __init__(self, base_url):
        self.base_url = base_url
    
    def construct_url(self, end_point, **params):
        """Helper function to construct API URLs dynamically"""
        query_params = "&".join([f"{key}={value}" for key, value in params.items()])
        return f"{self.base_url}{end_point}?{query_params}"

    def driver_info(self, end_point, session):
        return self.construct_url(end_point, session_key=session)

    def meeting_url(self, end_point, year):
        return self.construct_url(end_point, year=year)

    def session_url(self, end_point, year):
        return self.construct_url(end_point, year=year)

    def racecontrol_url(self, end_point, start_time, end_time):
        return self.construct_url(end_point, date_start=start_time, date_end=end_time)

    def lap_url(self, end_point, session_key):
        return self.construct_url(end_point, session_key=session_key)

    def weather_url(self, end_point, session_key):
        return self.construct_url(end_point, session_key=session_key)

    def api_call(self, url):
        """Making the API request and handles the response"""
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"Error: Received {response.status_code} from API")
            return None 
        return response.json()
baseURL = 'https://api.openf1.org/v1/'
def driver_ingestion():
    api = RawDataIngestion(baseURL)
    cursor.execute('select distinct sessionkey from f1_sessions')
    session_keys = cursor.fetchall()
    for session_key in session_keys:
        ssession = session_key[0]
        driver_url = api.driver_info("drivers", session=ssession)
        ddriverinfo = api.api_call(driver_url)
        print(driver_url)
        print(len(ddriverinfo))

        for data in ddriverinfo:
            cursor.execute(insert_sql1, (data['driver_number'], data['broadcast_name'], data['country_code'], data['first_name'], data['last_name'],
                                        data['full_name'], data['headshot_url'], data['meeting_key'], data['name_acronym'], data['session_key'],
                                        data['team_colour'], data['team_name']))
            print('added successfully')
        conn.commit()
    
def meeting_ingestion():
    api = RawDataIngestion(baseURL)
    meeting_url = api.meeting_url('meetings', year=2023)
    meeting_data = api.api_call(meeting_url)
    print(len(meeting_data))
    print(meeting_url)
    
    for data in meeting_data:
        date = parser.parse(data['date_start']).strftime('%Y-%m-%d %H:%M:%S')
        print(date)
        cursor.execute(insert_sql2, (data['meeting_key'], data['meeting_name'], data['country_name'], date, data['circuit_short_name'], data['location'], data['year']))
        conn.commit()
        
def session_ingestion():
    api = RawDataIngestion(baseURL)
    years = [2023,2024]
    for year in years:
        meeting_url = api.meeting_url('sessions', year=year)
        session_data = api.api_call(meeting_url)
        print(meeting_url)
        
        for data in session_data:
            startdate = parser.parse(data['date_start']).strftime('%Y-%m-%d %H:%M:%S')
            enddate = parser.parse(data['date_end']).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(insert_sql3, (data['session_key'], data['session_name'],data['session_type'], data['meeting_key'],
                            startdate, enddate))
            conn.commit()
            
def RaceControl():
    full_url = f'https://api.openf1.org/v1/race_control?date>=2023-01-01&date<2024-12-31'
    response = requests.get(full_url)
    print(full_url)
    if response.status_code == 200:
        datas = response.json()
        print(len(datas))
        print(datas[0])
        insert_data = [(data['session_key'], data['driver_number'], data['category'],
                                                  parser.parse(data['date']).strftime('%Y-%m-%d %H:%M:%S') , data['flag'], data['lap_number'],data['message'][:255], data['scope'])
                       for data in datas]
        cursor.executemany(insert_raceControl, insert_data )
        conn.commit() 
        
def Lapdata():
    cursor.execute('select DISTINCT SessionKey from f1_sessions')
    sessionkeylist = cursor.fetchall()
    sessionlist = [sessionlist[0] for sessionlist in sessionkeylist]
    print(len(sessionlist))
    for session in sessionlist:
        full_url = f'https://api.openf1.org/v1/laps?session_key={session}'
        print(full_url)
        response = requests.get(full_url)
        if response.status_code == 200:
            datas = response.json()
            print(len(datas))
            for data in datas:
                cursor.execute(insert_laprecords, (data['driver_number'],data['session_key'], data['lap_number'],
                                                     data['lap_duration'], data['duration_sector_1'], data['duration_sector_3'], data['duration_sector_3'],
                                                     data['st_speed']))
            conn.commit()
        
RaceControl()
Lapdata()
cursor.close()
conn.close()