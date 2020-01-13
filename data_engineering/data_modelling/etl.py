import psycopg2
from sql_queries import song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert, song_select
import os
import glob
from pyspark.sql import SparkSession
import datetime

def process_data(cur, conn, spark1, filepath, func):
    # find files
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, spark1, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    return all_files


def process_song_file(cur,spark1,filepath):
    # read json files
    df =  spark1.read.json(filepath)
    df.createOrReplaceTempView('song_data')

    # insert song records
    df_song = spark1.sql('SELECT song_id,title,artist_id,year,duration from song_data')
    song_data_dict = df_song.head().asDict()
    song_data_list = [*song_data_dict.values()]
    cur.execute(song_table_insert, song_data_list)

    # insert artist records
    df_artist = spark1.sql('SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude from song_data')
    artist_data_dict = df_artist.head().asDict()
    artist_data_list = [*artist_data_dict.values()]
    cur.execute(artist_table_insert, artist_data_list)


def process_log_file(cur,spark1,filepath):
    # read json files
    df =  spark1.read.json(filepath)
    df.createOrReplaceTempView('log_data')

    # insert user records
    df_user = spark1.sql('SELECT userId,firstName,lastName,gender,level FROM log_data')
    song_data_dict = [row.asDict() for row in df_user.collect()]
    for records in song_data_dict:
        user_data_list = [*records.values()]
        if "" in user_data_list:
            break
        cur.execute(user_table_insert,user_data_list)

    # insert date records
    df_ts = spark1.sql('SELECT ts FROM log_data where page=="NextSong"')
    song_data_dict = [row.asDict() for row in df_ts.collect()]
    for records in song_data_dict:
        date_list = []
        date = datetime.datetime.fromtimestamp([*records.values()][0]/1000.0)
        date_list = [date.strftime("%c"),date.strftime("%H"),date.strftime("%d"),date.strftime("%W"),date.strftime("%-m"),date.strftime("%Y"),date.strftime("%w")]
        cur.execute(time_table_insert,date_list)
    
    # insert songplay records
    index = 0
    for row in df.rdd.collect():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index,datetime.datetime.fromtimestamp(row.ts/1000.0), row.userId, row.level, songid, artistid, row.sessionId,\
                     row.location, row.userAgent)
        if "" in songplay_data:
            break
        cur.execute(songplay_table_insert, songplay_data)
        index = index+1

    

        
def main():
    conn = psycopg2.connect(database="sparkifydb", user="bro", password="n9l1b8r1p", host="127.0.0.1", port="5432")
    cur = conn.cursor()

    spark1 = SparkSession.builder.appName('etl').getOrCreate()

    process_data(cur, conn, spark1, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, spark1, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()