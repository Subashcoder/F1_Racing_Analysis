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