from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json, col
from datetime import datetime

# Define the schema for the song messages
songSchema = StructType([
    StructField("listener", StringType(), False),
    StructField("time", TimestampType(), False),
    StructField("song", StringType(), False),
])

# Initialize SparkSession
spark = SparkSession.builder.appName("SSKafka").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka stream
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:29092").option("subscribe", "spotify-testing").option("startingOffsets", "latest").load()

# Parse JSON data and select required fields
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), songSchema).alias("data")).select("data.*")


# Define function to persist data to Cassandra
def writeToCassandra(write_df, batch_id):
    
    songs_df = spark.read.csv("spotify-songs.csv", header=True)

    write_df = write_df.join(songs_df,write_df.song == songs_df.name, "inner").select("listener","time","danceability","song", "tempo","acousticness","album_name","album_release_date","artists","duration_ms","energy","instrumentalness","key","liveness","loudness","mode","speechiness","valence")
    # Write data to Cassandra
   
    write_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="songs_listens_test", keyspace="spotify").save()


    
result = None
while result is None:
    try:
        # connect
        result = parsed_df.writeStream.option("spark.cassandra.connection.host","localhost:9042").foreachBatch(writeToCassandra).outputMode("update").start().awaitTermination()
    except:
         pass
