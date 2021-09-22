import pandas as pd
import datetime
from datetime import datetime,timedelta
from pymongo import MongoClient
import mysql
from mysql.connector import Error


CLIENT = MongoClient('mongo_airflow',27017, maxPoolSize=50)
DB = CLIENT['spotify']
COLLECTION = DB['playlist']



def load_data_to_mysql(track_df,song_df,album_df,artist_df):
    try:
        connection = mysql.connector.connect(host='mysql_airflow',
                                            database='spotify',
                                            user='admin',
                                            password='admin')
        cursor = connection.cursor()
        print("Connected to mysql")
    except Error as e:
        print("Failed to connect mysql, {}".format(e))

    query_table_track = """
        CREATE TABLE IF NOT EXISTS tracks(
            id INT(50) AUTO_INCREMENT PRIMARY KEY,
            song_id VARCHAR(50),
            played_at VARCHAR(50),
            timestamps DATE,
            FOREIGN KEY (song_id) REFERENCES songs(id),
            CONSTRAINT UC_tracks UNIQUE (id)
        )
        """
    query_table_song = """
        CREATE TABLE IF NOT EXISTS songs(
            id VARCHAR(50) PRIMARY KEY, 
            song_name VARCHAR(50),
            song_duration INT(10),
            song_popularity INT(10),
            song_url TEXT,
            album_id VARCHAR(50),
            FOREIGN KEY (album_id) REFERENCES albums(id),
            CONSTRAINT UC_songs UNIQUE (id)
        )
        """
    query_table_album = """
        CREATE TABLE IF NOT EXISTS albums(
            id VARCHAR(50) PRIMARY KEY,
            album_name VARCHAR(50),
            album_release DATE,
            album_total_tracks INT(5),
            album_url TEXT,
            album_image TEXT,
            artist_id VARCHAR(50),
            FOREIGN KEY (artist_id) REFERENCES artists(id),
            CONSTRAINT UC_albums UNIQUE (id)
        )
        """
    query_table_artist = """
        CREATE TABLE IF NOT EXISTS artists(
            id VARCHAR(50) PRIMARY KEY,
            artist_name VARCHAR(50),
            artist_type VARCHAR(25),
            artist_url TEXT,
            CONSTRAINT UC_artists UNIQUE (id)
        )
        """

    if connection.is_connected():
        cursor.execute("USE spotify")
        cursor.execute(query_table_artist)
        cursor.execute(query_table_album)
        cursor.execute(query_table_song)
        cursor.execute(query_table_track)


        for data in artist_df.values:
            cursor.execute("""INSERT IGNORE INTO artists (id, artist_name, artist_type, artist_url)
                        VALUES ("{artist_id}","{artist_name}", "{artist_type}", "{artist_url}"); 
                        """.format(artist_id = data[0],
                                artist_name = data[1],
                                artist_type = data[2],
                                artist_url= data[3]))
            connection.commit()

        for data in album_df.values:
            cursor.execute(""" INSERT IGNORE INTO albums (id, album_name, album_release, album_total_tracks, album_url, album_image, artist_id)
                        VALUES ("{album_id}", "{album_name}", "{album_release}", {album_total_tracks}, "{album_url}", "{album_image}" , "{artist_id}")
                        """.format(album_id = data[0],
                                album_name = data[1],
                                album_release = data[2],
                                album_total_tracks= data[3],
                                album_url = data[4],
                                album_image = data[5],
                                artist_id = data[6]))
            connection.commit()

        for data in song_df.values:
            cursor.execute(""" INSERT IGNORE INTO songs (id, song_name, song_duration, song_popularity, song_url, album_id) 
                            VALUES ( "{song_id}", "{song_name}", {song_duration}, {song_popularity}, "{song_url}", "{album_id}")
                            """.format(song_id = data[0],
                                    song_name = data[1],
                                    song_duration = data[2],
                                    song_popularity = data[3],
                                    song_url = data[4],
                                    album_id = data[5]))
            connection.commit()

        for data in track_df.values:
            cursor.execute(""" INSERT IGNORE INTO tracks (song_id,played_at,timestamps)
                            VALUES ("{song_id0}", "{played_at}", "{timestamps}" )
                            """.format(song_id0=data[0],
                                    played_at=data[1],
                                    timestamps=data[2]))
            connection.commit()

        print("Store data to mysql successfuly")

def check_if_valid_data(playlist):
    #check_if_empty
    if len(playlist) == 0:
        print("No File downloaded, finishing execution")
        return False,playlist
    print("{} Playlist Load Successfuly".format(len(playlist)))
    #Primary key check
    key_temp = []
    new_data = []
    isDuplicate = False
    noDuplicate = 0
    for data in playlist:
        if data['played_at'] not in key_temp:
            new_data.append(data)
            key_temp.append(data['played_at'])
        else:
            noDuplicate += 1
            isDuplicate = True
    if isDuplicate:
        print("{} Duplicate data has been removed".format(noDuplicate))
    else:
        print("No data duplication")

    return True,new_data


def load_data_from_mongodb():
    yesterday = datetime.now() -  timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    try:
        collect = COLLECTION.find({'timestamp':yesterday})
        playlist_json = []
        for data in collect:
            playlist_json.append(data)
        
    except Exception as e:
        print("Failed to Load spotify playlist, {}".format(e))
    return playlist_json

def json_to_dataframe(playlist):
    played_at = []
    timestamps = []
    song_id = []
    song_name = []
    song_duration = []
    song_popularity = []
    song_url = []

    artist_id = []
    artist_name = []
    artist_type = []
    artist_url = []

    album_id = []
    album_name =[]
    album_release =[]
    album_total_tracks =[]
    album_url =[]
    album_image= []

    for data in playlist:
        played_at.append(data['played_at'])
        timestamps.append(data['timestamp'])

        song_id.append(data['song']['song_id'])
        song_name.append(data['song']['song_name'])
        song_duration.append(data['song']['song_duration'])
        song_popularity.append(data['song']['song_popularity'])
        song_url.append(data['song']['song_url'])

        artist_id.append(data['artist']['artist_id'])
        artist_name.append(data['artist']['artist_name'])
        artist_type.append(data['artist']['artist_type'])
        artist_url.append(data['artist']['artist_url'])

        album_id.append(data['album']['album_id'])
        album_name.append(data['album']['album_name'])
        album_release.append(data['album']['album_release'])
        album_total_tracks.append(data['album']['album_total_tracks'])
        album_url.append(data['album']['album_url'])
        album_image.append(data['album']['album_image'])

    #create 3 table: track, song, album, artist
    track_dict = {'song_id':song_id,
                'played_at':played_at,
                'timestamp':timestamps}
    song_dict = {'id':song_id,
                'song_name':song_name,
                'song_duration':song_duration,
                'song_popularity':song_popularity,
                'song_url':song_url,
                'album_id':album_id}
    album_dict = {'id':album_id,
                'album_name':album_name,
                'album_release':album_release,
                'album_total_tracks':album_total_tracks,
                'album_url':album_url,
                'album_image':album_image,
                'artist_id':artist_id}
    artist_dict = {'id':artist_id,
                'artist_name':artist_name,
                'artist_type':artist_type,
                'artist_url':artist_url}

    track_df = pd.DataFrame.from_dict(track_dict)
    song_df = pd.DataFrame.from_dict(song_dict)
    artist_df = pd.DataFrame.from_dict(artist_dict)
    album_df = pd.DataFrame.from_dict(album_dict)

    track_df['timestamp'] = pd.to_datetime(track_df['timestamp'])
    album_df['album_release'] = pd.to_datetime(album_df['album_release'])

    return track_df,song_df,album_df,artist_df


def delete_duplication(df):
    new_df = df.drop_duplicates(subset=['id'])
    return df


def extract_transform_load():

    # Extract data from mongo db
    playlist = load_data_from_mongodb()
    is_Valid , new_playlist = check_if_valid_data(playlist)

    # Transform data from json format to table realtional sql
    if is_Valid:
        track_df, song_df, album_df, artist_df = json_to_dataframe(new_playlist)
        song_df = delete_duplication(song_df)
        album_df = delete_duplication(album_df)
        artist_df = delete_duplication(artist_df)

    # Load data to mysql
        load_data_to_mysql(track_df, song_df, album_df, artist_df)


    
