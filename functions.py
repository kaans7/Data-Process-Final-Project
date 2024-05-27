import numpy as np
import pandas as pd
import psycopg2
from requests import post,get
import base64
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time

CLIENT_ID="3a6f7234b486450d9e7f2ba0fe7add20"
CLIENT_SECRET="ce02498c58e14c88bc570f1d3882dbd3"

client_credentials_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

def get_token():
    """
    Generates and returns an authentication token using the client credentials.
    """
    auth_string=CLIENT_ID+':'+CLIENT_SECRET
    auth_bytes=auth_string.encode('utf-8')
    auth_base64=str(base64.b64encode(auth_bytes),'utf-8')
    
    url = "https://accounts.spotify.com/api/token"
    headers= {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded"
        }
    data={"grant_type": "client_credentials"}
    result = post(url,headers=headers,data=data)
    json_result=json.loads(result.content)
    token=json_result["access_token"]
    return token

def get_auth_header(token):
    """
    Creates and returns the authorization header using the given token.
    """
    return {"Authorization": "Bearer "+token}

def search_for_artist(token,artist_name):
    """
    Searches for an artist by name and returns the first matching artist's information.
    """
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"q={artist_name}&type=artist&limit=1"
    
    query_url= url+"?"+query
    result = get(query_url,headers=headers)
    json_result=json.loads(result.content)["artists"]["items"]
    return json_result[0]

def search_for_track(token,track_name):
            url = "https://api.spotify.com/v1/search"
            headers = get_auth_header(token)
            query = f"q={track_name}&type=track&limit=1"
            
            query_url= url+"?"+query
            result = get(query_url,headers=headers)
            json_result=json.loads(result.content)["tracks"]["items"][0]
            # print(json_result)
            return json_result
