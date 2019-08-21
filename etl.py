import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """    
    # open song file
    df = pd.read_json(filepath,lines="true")

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values
    song_data=(song_data[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data=(artist_data[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes the log file whose filepath has been provided as an arugment.
    It extracts the user and time information in order to store it into the user and time tables respectively.
    Then it uses the song_name and artist_name from the log file to lookup the song_id and artist_id 
    and inserts the information into the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """    
    # open log file
    df = pd.read_json(filepath,lines="true")

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts']
    time = pd.to_datetime(t,unit='ms')
    
    # insert time data records
    time_data = (time.dt.time, time.dt.hour, time.dt.day, time.dt.weekofyear, time.dt.month, time.dt.year, time.dt.weekday)
    column_labels = ("timezone","hour","day","week","month","year","weekday")
    time_dictionary = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame.from_dict(time_dictionary)     
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = 'None', 'None'

        # insert songplay record
        row.ts=pd.to_datetime(row.ts/1000)
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This is the primary procedure which invokes the procs for processing the song and log data files.
    The song file and log file information is passed as parameters to the process() procedure.

    INPUTS: 
    * cur  - the cursor variable
    * conn - database conenction variable
    * filepath the file path to the data file
    * func - procedure name which processes the data file
    """     
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This is the main procedure where the program execution begins.
    """     
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()