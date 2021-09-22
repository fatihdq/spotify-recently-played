import json
import requests
import datetime
from datetime import datetime,timedelta
from pymongo import MongoClient, errors

## get token on spotify website: https://developer.spotify.com/console/get-recently-played/
TOKEN = "BQAxEKRy3XW5Gc_ej_kvPB6U0NrS0woqZLolcl4VPi_FUQO_VUNhEqfNkTmvzpjvvTPKLODe6DffeTpEqM9zNP_fxxA6FtPTuM5akJiuJjrVP5lsveUC6Pq9yJyl9ghqUz-GauWk7_Bvuwwxq3LkiHedVDUF19nywedCUk-uWNHh-LhLkuc3"
CLIENT = MongoClient('mongo_airflow',27017, maxPoolSize=50)
DB = CLIENT['spotify']
COLLECTION = DB['playlist']



def save_to_mongodb(song_list):
    if isinstance(song_list, list):
        try:
            COLLECTION.insert_many(song_list)
            print("File saved successfully")
        except Exception as e:
            print("Cannot save to mongodb {}".format(e))
    else:
        try:
            COLLECTION.insert_one(song_list)
            print("File saved successfully")
        except Exception as e:
            print("Cannot save to mongodb {}".format(e))
    

def check_if_not_empty(song_list) ->bool:
    if len(song_list) ==0 :
        print("No song downloaded, Finishing execution")
        return False
    print("{} Song downloaded".format(len(song_list)))
    return True

def colect_data_from_api():
    headers= {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    song_list = []


    try:
        req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)
        data = req.json()
    
        if check_if_not_empty(data['items']):
            
            for song  in data['items']:
                song_list.append({'song':{'song_id':song['track']['id'],
                                        'song_name':song['track']['name'],
                                        'song_duration':song['track']['duration_ms'],
                                        'song_popularity':song['track']['popularity'],
                                        'song_url':song['track']['external_urls']['spotify']},
                                'artist':{'artist_id':song['track']['album']['artists'][0]['id'],
                                        'artist_name':song['track']['album']['artists'][0]['name'],
                                        'artist_type':song['track']['album']['artists'][0]['type'],
                                        'artist_url':song['track']['album']['artists'][0]['external_urls']['spotify']},
                                'album':{'album_id':song['track']['album']['id'],
                                        'album_name':song['track']['album']['name'],
                                        'album_release':song['track']['album']['release_date'],
                                        'album_total_tracks':song['track']['album']['total_tracks'],
                                        'album_url':song['track']['album']['external_urls']['spotify'],
                                        'album_image':song['track']['album']['images'][0]['url']},
                                'played_at':song['played_at'],
                                'timestamp':song['played_at'][0:10]})
    except Exception as e:
        print("Failed to request api, {}".format(e))
    
    return song_list

def data_collection():
    song_list = colect_data_from_api()
    save_to_mongodb(song_list)


    