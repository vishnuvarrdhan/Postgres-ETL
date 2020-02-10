1. Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

    Sparkify is a startup application based on music streaming. The analytics team at Sparkify wants to analyze the trends on songs       data that has been collected and also about the songs their users are listening to. As the data is collected is in the form of       json logs sourced from their application they need this data to be stored in database in a way, on which they can easily query       the data.
    
2. State and justify your database schema design and ETL pipeline.
    
    Based on the needs of Sparkify team, i had proposed and designed a database schema. Data sources songs and user_log are considered and developed a design based on star schema in which it has 4 dimension tables and 1 fact table as descirbed below.
    
    a. songplays(Fact): It has all the information related to which songs user is listening to. Overall it has following columns
        
        songplay_id : It is an PRIMARY KEY which can be used to retrieve the rows uniquely from this table
        start_time  : It is the timestamp at which user started listening to the song.
        user_id     : It is an Identifier through which we can obtain details about the user like firstname, lastname, gender etc..,
        level       : It is about the wheather user is premium user or not
        song_id     : It is an Identifer through which we can obtain details about the song like song name, year released etc..,
        artist_id   : It is an Identifer through which we can obtain details about artist like name, location etc..,
        session_id  : details about the session of the user
        location    : location from which user is listening to the song
        user_agent  : Deatils about the user browser, operating system and about the device used to listen song.
        
    b. users(dimension): It has details about the user like user_id(PRIMARY_KEY), firstName, lastName, level.
    
    c. songs(dimension): It has detail about the song like songName, yearReleased, duration.
    
    c. artists(dimension): It has details about the song artists like artist_id(PRIMARY KEY), name, location, latitude, longitude.
    
    d. time(dimension): THe ts column in the log data dataset broken down into  hour, day, week, month, year, weekday.
    
    I had also designed an ETL pipeline which transfers data form log_files and inserts records in  songplays table and users table. Data from song dataset is extracted and stored the required details in songs and artists tables.

        
        
