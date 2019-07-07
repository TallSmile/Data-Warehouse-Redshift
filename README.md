# Data Warehouse in Redshift

The purpose of this project is to analyze data simulated by [eventsim](https://github.com/Interana/eventsim). The simulated data consists of songs and user activity from music streaming app. The main goal of the project is to  create a redshift database with tables designed to optimize queries on song play analysis. Using the song and log datasets, an ETL pipeline is used to create star schema for this analysis.

## Getting Started

Before proceeding with deployment please create a AWS redshift instance with read access to S3 storage.

## Deployment

The project should be deployed on AWS. Configuration variables are read from `dwh.cfg`.

* Copy example configuration file and fill it in.

```Shell
cp dwh.example.cfg dwh.cfg
```

* Create tables in DW.

```Shell
python create_tables.py
```

* Run ETL pipeline.

```Shell
python etl.py
```

> Note that you can use debugging flag ( `python -D`... ) to see SQL commands that are executed. In addition, you can use command line arguments in order to execute only specific commands ( see `python create_tables.py -h` and `python etl.py -h` )

## Schema for Song Play Analysis

Below is presented the schema of sparkifydb. In order to optimize queries for song play analysis and reduce data duplication, **star scheme** is used in this DB.

> Note that schema is distributed in a way to optimize queries on `songplays` table. Dimension tables are distributed to all nodes in the cluster.

### Fact Table

Records in log data associated with song plays i.e. records with page NextSong

| **songplays** | KEY|
|---------------|---:|
| songplay_id   |  PK|
| start_time    |  FK|
| user_id       |  FK|
| level         |    |
| song_id       |  FK|
| artist_id     |  FK|
| session_id    |    |
| location      |    |
| user_agent    |    |

### Dimension Tables

Users in the app

| **users** |KEY|
|-----------|--:|
| user_id   | PK|
| first_name|   |
| last_name |   |
| gender    |   |
| level     |   |

Songs in music database

| **songs** |KEY|
|-----------|--:|
| song_id   | PK|
| title     |   |
| artist_id | FK|
| year      |   |
| duration  |   |

Artists in music database

| **artists**  | KEY|
|--------------|---:|
| artist_id    | PK |
| name         |    |
| location     |    |
| lattitude    |    |
| longitude    |    |

Timestamps of records in songplays broken down into specific units

| **time**     | KEY|
|--------------|---:|
| start_time   | PK |
| hour         |    |
| day          |    |
| week         |    |
| month        |    |
| year         |    |
| weekday      |    |

## TODO

Allow programmic creation of Redshift cluster by finish `create_cluter.py` script.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
