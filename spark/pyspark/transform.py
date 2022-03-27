import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType

from google.cloud import bigquery

import wget
import os


if __name__ == "__main__":
    wget.download("https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar", out="/spark/gcs-connector-hadoop3-latest.jar")
    wget.download("https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar", out="/spark/spark-bigquery-latest_2.12.jar")

    google_project_id = os.getenv('GOOGLE_PROJECT_ID')
    google_biquery_dataset = os.getenv('GOOGLE_BIGQUERY_DATASET')
    google_cloud_storage_id = os.getenv('GOOGLE_CLOUD_STORAGE_ID')
    
    # Spark context and spark session.
    
    conf = SparkConf().setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/spark/gcs-connector-hadoop3-latest.jar,/spark/spark-bigquery-latest_2.12.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/.google/google_credentials.json")

    sc = SparkContext(conf=conf)

    sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", "/.google/google_credentials.json")
    sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
    
    spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
    
    # Load the data from the bucket
    df_million_song = spark.read \
    .option("header", "true") \
    .parquet(f"gs://{google_cloud_storage_id}/raw/millionsongsubset.parquet")

    df_million_song = df_million_song.repartition(4)
    
    df_million_song = df_million_song.withColumn('tempo_int', (df_million_song.tempo).cast(IntegerType())).drop('tempo')
    
    df_to_save = df_million_song.drop('bars_confidence','bars_start','beats_confidence', 'beats_start', 'sections_confidence', 'sections_start', 'segments_confidence', 'segments_loudness_max', 'segments_loudness_max_time', 'segments_loudness_start', 'segments_start')
    
    record = df_to_save.sample(False, 0.1, seed=0).limit(5)
    
    record.write \
        .format("bigquery") \
        .option("temporaryGcsBucket",google_cloud_storage_id) \
        .option('table', f"{google_biquery_dataset}.data") \
        .save()
        
    
    client = bigquery.Client()

    dataset_ref = client.dataset(google_biquery_dataset)
    table_ref = dataset_ref.table("data")
    table = client.get_table(table_ref)  # API Request

    # View table properties
    schema = table.schema

    table_id = f"{google_project_id}.{google_biquery_dataset}.data_partitioned"

    table = bigquery.Table(table_id, schema=schema)
    table.range_partitioning = bigquery.RangePartitioning(
        # To use integer range partitioning, select a top-level REQUIRED /
        # NULLABLE column with INTEGER / INT64 data type.
        field="year",
        range_=bigquery.PartitionRange(start=0, end=2200, interval=10),
    )

    table.clustering_fields = ["tempo_int", "mode", "key"]

    # TODO(developer): Set table_id to the ID of the destination table.
    # table_id = "your-project.your_dataset.your_table_name"


    table = client.create_table(table)  # Make an API request.
    df_to_save.write \
        .mode('append') \
        .format("bigquery") \
        .option("temporaryGcsBucket",google_cloud_storage_id) \
        .option('table', f"{google_biquery_dataset}.data_partitioned") \
        .save()