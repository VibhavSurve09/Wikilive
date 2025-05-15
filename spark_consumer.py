from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,BooleanType
import os

load_dotenv()


def write_event_to_db(batch_df,batch_id):
    batch_df.write \
        .format('jdbc') \
        .option('url','jdbc:postgresql://{}/{}'.format(os.getenv('POSTGRES_URL'),os.getenv('POSTGRES_DB'))) \
        .option("user", os.getenv('POSTGRES_USER')) \
        .option("password", os.getenv('POSTGRES_PASSWORD')) \
        .option('dbtable','LIVE_WIKIEVENTS') \
        .option("driver", "org.postgresql.Driver") \
        .mode('append') \
        .save()
    
def live_feed(spark):
    live_edits_df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', os.getenv('KAFKA_BOOTSTRAP_SERVER')) \
        .option('subscribe', os.getenv('KAFKA_TOPIC')) \
        .load()
    #Enforcing schema to avoid issues    while streaming       
    events_schema=StructType([
        StructField('type',StringType()),
        StructField('is_bot',BooleanType()),
        StructField('server_name',StringType()),
        StructField('user_name',StringType()),
        StructField('minor',StringType()),
        StructField('title_url',StringType()),
        StructField('namespace',StringType()),
        StructField('comment',StringType()),
    ])
    event_stream_df=live_edits_df \
        .select(from_json(col('VALUE') \
            .cast('string'),schema=events_schema) \
                .alias("event_stream"),col('TIMESTAMP') \
                    .cast('TIMESTAMP').alias('TIMESTAMP'))
    event_data_df=event_stream_df \
    .select('event_stream.type','event_stream.is_bot','event_stream.server_name','event_stream.user_name','event_stream.minor','event_stream.title_url','event_stream.namespace','event_stream.comment',col('TIMESTAMP').alias('event_timestamp'))
    
    #event_data_df.writeStream.format('console').outputMode("append").start().awaitTermination()
    #key=live_edits_df.selectExpr('CAST(TIMESTAMP AS TIMESTAMP)')
    # For testing purposes
    event_data_df.writeStream.foreachBatch(write_event_to_db).outputMode("append").start().awaitTermination()

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Wikilive") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    live_feed(spark)
