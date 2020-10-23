## Sparkify star schema

The project scheme aims to be an implementation of Sparkify, 
a Json file architecture, to have a star model.
This is intended to be the start for a Data-lake to Sparkify APP

### Purpose

The architecture that Sparkify implements is based on writing JSON files with information  
about songs and information about the users who listen to them, about which it can be useful to have a datalake to request
data easily.
it allows you to generate information and store generating very little lock against disk with high speed.     
another advantage of implementing a data lake is that it allows you to work with unstructured and structured data with relative ease .

This project aims to create an **Data Lake** reporting and ML purposes.   
Making aggregated queries and taking consolidated information easier by SparkSQL. 


### Database Schema 

The proposed scheme is a star type scheme, with 4 dimension tables,  
This approach allows you to take full advantage of the flexibility of Data-lake.


### Run ETL
Execute:
```
python etl.py
```

### Query Samples

Make querys using spark is so easy, some samples below:

#### Query to find top artists by number of songs

```python
artists_df = spark.read.parquet("s3a://s3-output-directory/artists")

song_df = spark.read.parquet("s3a://s3-output-directory/songs")

artists_df.createOrReplaceTempView("staging_artists")

song_df.createOrReplaceTempView("staging_songs")

spark.sql('''
        select name,count(1) from staging_songs
                    join staging_artists using (artist_id)
                    group by 1 
                    order by 2 desc

                    ''').toPandas().head(12)
```

https://github.com/mrdavidhidalgo/-Udacity-Data-Engineering-Nanodegree-Spark-Project/blob/master/img/query1.png



 