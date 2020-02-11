import os
import json
import datetime
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):    
    
    """Walks through all files nested under filepath, and processes all files found. Stores the data in song and artist tables

    Parameters:
        cur (psycopg2.cursor()): Cursor of the sparkifydb database
        filepath (str): Filepath parent of the logs to be analyzed

    Returns:
        Nothing
    """
    # open song file
    with open(filepath, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame([data])

    # insert song record
    song_data = df.values.tolist()
    song_data = [song_data[0][7],song_data[0][8],song_data[0][0],song_data[0][9],song_data[0][5]]

    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.values.tolist()
    artist_data = [artist_data[0][0],artist_data[0][4],artist_data[0][2],artist_data[0][1],artist_data[0][3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Walks through all files nested under filepath, and processes all log files found. It stores data into users, song_plays, time tables.

    Parameters:
        cur (psycopg2.cursor()): Cursor of the sparkifydb database
        filepath (str): Filepath parent of the logs to be analyzed

    Returns:
        Nothing
    """
    # open log file
    data = []
    for line in open(filepath,'r'):
        data.append(json.loads(line))
    df = pd.DataFrame(data)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = {'start_time' : t.tolist(),
             'hour':t.dt.hour,
             'day':t.dt.day,
             'week':t.dt.week,
             'month':t.dt.month,
             'year':t.dt.year,
             'weekday':t.dt.weekday}
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.filter(['userId','firstName','lastName','gender','level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        timeToDat = pd.to_datetime(row.ts,unit='ms')
        # get songid and artistid from song and artist tables
        cur.execute(song_select, [row.song, row.artist, row.length])
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (timeToDat,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Walks through all files nested under filepath, and processes all logs found.

    Parameters:
        cur (psycopg2.cursor()): Cursor of the sparkifydb database
        conn (psycopg2.connect()): Connection to the sparkifycdb database
        filepath (str): Filepath parent of the logs to be analyzed
        func (python function): Function to be used to process each log

    Returns:
        Name of files processed
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
