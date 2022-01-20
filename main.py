from os import fwalk
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By

#===============================================//=========================================================
# First of all, we need to automatically get the token access to the API

# Accessing the page

URL = 'https://developer.spotify.com/console/get-recently-played/?limit=50&after=&before='

wd = webdriver.Chrome(executable_path='(my_path)/chromedriver')
wd.get(URL)

# Finding the Token button and clicking on it

wd.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/div[2]/div/div/form/div[3]/div/span/button').click()

# Clicking on the user-read-recently pop-up and then on request token
check_button = wd.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/div[2]/div/div/div[1]/div/div/div[2]/form/div[1]/div/div/div/div/label/span')
wd.execute_script("arguments[0].click();", check_button)

request_token = wd.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/div[2]/div/div/div[1]/div/div/div[2]/form/input')
wd.execute_script("arguments[0].click();", request_token)

# Filling username and password
username = wd.find_element(By.ID, 'login-username')
password = wd.find_element(By.ID, 'login-password')

username.send_keys('(my_username)')
password.send_keys('(my_password)')

submit = wd.find_element(By.XPATH, '/html/body/div[1]/div[2]/div/form/div[4]/div[2]/button')
wd.execute_script("arguments[0].click();", submit)

# Finally getting token
wd.implicitly_wait(10)
token_link = wd.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/div[2]/div/div/form/div[3]/div/input').get_attribute('value')

#===============================================//=========================================================

# Now, we set our database and actually start the ETL process

DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = '(my_username)'
TOKEN =  token_link

# Creating a function to validate the data we get is importante, so let's do this

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print('No songs downloaded. Finishing execution.')
        return False

    # Primary Key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key check is violated.')

    # Check for nulls
    if df.isnull().values.any():
        raise Exception('Null values found.')

    # Check that all timestapms are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') < yesterday:
            raise Exception('At least one of the returned songs does not come from within the last 24 hours')

    return True

#===============================================//=========================================================

if __name__ == '__main__':

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

    song_dic = {
        'song_name' : song_names,
        'artist_name' : artist_names,
        'played_at' : played_at_list,
        'timestamp' : timestamps
    }

    song_df = pd.DataFrame(song_dic, columns=['song_name', 'artist_name', 'played_at', 'timestamp'])

    print(song_df)

    # Validate
    if check_if_valid_data(song_df):
        print('Data valid, proceed to Load stage.')

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = '''
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    '''
    cursor.execute(sql_query)
    print('Opened database successfully.')

    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except:
        print('Data already exists in database.')

    cursor.close()
    print('Closed database successfully.')


    # Job scheduling
    # TODO: automate the scheduling with DAGs in Airflow