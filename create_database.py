import numpy as np
import pandas as pd
import psycopg2
from requests import post,get
import base64
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time

from functions import get_token,get_auth_header,search_for_artist
def execute_commit(query):
    
    cur.execute(query)
    conn.commit()
'''
En popüler 50 şarkı
En çok bulunan sanatçılar
Genre dağılımı(Pie Chart)
Şarkıların ortalama uzunluğu
Albümlerin tarih analizi
Albümlerin kaç şarkısı olduğu bar graph
Sanatçıların populerlik analizi'''

CLIENT_ID="bbeade3ebc754cd0ae2721fe0df9132b"
CLIENT_SECRET="0c70e5c244d54820b83c7d853514cd53"

client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)


token=get_token()

f= open("password.txt",'r')
pssword=f.read()

conn=None
cur=None

#Connect to SQL
conn = psycopg2.connect(
    database='dataproject',
    host="localhost",
    user="postgres",
    password=pssword,
    port=5432)

cur =conn.cursor()

#Query to create artist_rock
create_artist_table='''CREATE TABLE IF NOT EXISTS artist_rock (
                        id      varchar(22) PRIMARY KEY,
                        artist_name    varchar(100) NOT NULL,
                        genres  varchar(100) NOT NULL,
                        followers int,
                        popularity int
                        )
                        ;'''
execute_commit(create_artist_table)



cur =conn.cursor()                           
#Query to create album_rock
create_album_table='''CREATE TABLE IF NOT EXISTS album_rock (
                        id      varchar(22) PRIMARY KEY,
                        album_name    varchar(200) NOT NULL,
                        artist_id varchar(22) NOT NULL,
                        CONSTRAINT artistid
		                    FOREIGN KEY(artist_id)
			                    REFERENCES artist_rock(id),
                        release_date  varchar(10) NOT NULL,
                        total_tracks int
                        )
                        ;'''  
execute_commit(create_album_table)



cur =conn.cursor()
#Query to create track_rock
create_track_table='''CREATE TABLE IF NOT EXISTS track_rock (
                        id      varchar(22) PRIMARY KEY,
                        track_name    varchar(200) NOT NULL,
                        duration_ms int,
                        popularity int,
                        album_id varchar(22) NOT NULL,
                        CONSTRAINT albumid
		                    FOREIGN KEY(album_id)
			                    REFERENCES album_rock(id),
                        artist_id varchar(22) NOT NULL,
                        CONSTRAINT trackid
		                    FOREIGN KEY(artist_id)
			                    REFERENCES artist_rock(id)
                        )
                        
                        ;'''
                        
execute_commit(create_track_table)


cur =conn.cursor()
#Query to create analyze_rock
create_analyze_table='''CREATE TABLE IF NOT EXISTS analyze_rock (
                        id      varchar(22) PRIMARY KEY,
                        acousticness    float,
                        danceability  float,
                        energy float,
                        instrumentalness float,
                        liveness float,
                        loudness float,
                        mode int,
                        speechiness float,
                        tempo float,
                        valence float
                        
                        )
                        ;'''
execute_commit(create_analyze_table)


cur =conn.cursor()
def clean_text(text):
    if "'" in text:
        text=text.replace("'","''")
    return text

#Loop to get every track from a given playlist, and save to the database
track_number=3820
while track_number<5400:     
    for track in sp.playlist_tracks('6SefiumTj6DBQPgP5i9Wxt',limit=100, offset=track_number)["items"]:
        track_number+=1
        print('Track:',track_number)
        
        #Get track name and clean the text that cause error on query
        track_name = clean_text(track["track"]["name"])
        
        #Get track_id
        track_id=track["track"]["id"]

        #Get album name and clean the text that cause error on query
        album = clean_text(track["track"]["album"]["name"])   

        #Get album id
        album_id = track["track"]["album"]["id"]
        
        #Get artist name and clean the text that cause error on query
        artist_name = clean_text(track["track"]["artists"][0]["name"])

        #Get artist id
        artist_id = track["track"]["artists"][0]["id"]
        
        #Get duration of the track
        duration_ms=  track["track"]["duration_ms"]
        #Get track popularity
        track_pop = track["track"]["popularity"]           
        #Get track url
        track_url=track["track"]["external_urls"].get('spotify')
       
        #Get artist genre
        artist_dic=search_for_artist(token,f'{artist_name}')
        if artist_dic['genres'] ==[]:
            genre=''
            
        else:
            genre=artist_dic['genres'][0]
        #Get artist follower
        followers=artist_dic['followers'].get('total')
        #Get artist popularity
        artist_pop=artist_dic['popularity']
        
        #Get album release date
        album_release_date= track["track"]["album"]["release_date"]
        #Get total tracks
        album_total_tracks=track["track"]["album"]["total_tracks"]
        
        #Get sound analyze variables
        track_analyze=sp.audio_features(f'{track_id}')[0]
        track_acousticness = track_analyze.get('acousticness')
        track_danceability = track_analyze.get('danceability')
        track_energy=track_analyze.get('energy')
        track_instrumentalness=track_analyze.get('instrumentalness')
        track_liveness=track_analyze.get('liveness')
        track_loudness=track_analyze.get('loudness')
        track_mode=track_analyze.get('mode')
        track_speechines=track_analyze.get('speechiness')
        track_tempo=track_analyze.get('tempo')
        track_valence=track_analyze.get('valence')
        
        
        #Query to insert artist data to artist_rock table    
        intert_artist_table =f''' 
                    INSERT INTO artist_rock (id,artist_name,genres,followers,popularity)
            
                    VALUES(
                        '{artist_id}','{artist_name}','{genre}','{followers}','{artist_pop}')
                    ON CONFLICT (id) DO NOTHING;'''
           
        cur.execute(intert_artist_table)
        
        
        #Query to insert album data to album_rock table     
        intert_album_table =f''' 
                    INSERT INTO album_rock (id,album_name,artist_id,release_date,total_tracks)
            
                    VALUES(
                        '{album_id}','{album}','{artist_id}','{album_release_date}','{album_total_tracks}')
                    ON CONFLICT (id) DO NOTHING;'''
        cur.execute(intert_album_table)

        #Query to insert track data to track_rock table 
        intert_track_table =f'''  
                    INSERT INTO track_rock (id,track_name,album_id,artist_id,duration_ms,popularity)
        
                    VALUES(
                        '{track_id}','{track_name}','{album_id}','{artist_id}',{duration_ms},{track_pop})
                    ON CONFLICT (id) DO NOTHING;'''
        cur.execute(intert_track_table)
         
        #Query to insert analyze data to analyze_rock table 
        insert_analyze_table=f''' 
                    INSERT INTO analyze_rock(id,acousticness,danceability,energy,instrumentalness,liveness,loudness,mode,speechiness,tempo ,valence) 
            
                    VALUES('{track_id}','{track_acousticness}','{track_danceability}','{track_energy}',
                           '{track_instrumentalness}','{track_liveness}','{track_loudness}','{track_mode}',
                           '{track_speechines}','{track_tempo}','{track_valence}')
                    ON CONFLICT (id) DO NOTHING;
            '''
     
        cur.execute(insert_analyze_table)
        conn.commit()
        time.sleep(0.25)