import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import types as T
from pyspark.sql.functions import col, explode
from pyspark.sql import functions as F

def create_spark_session():
    """Initialize Spark session."""
    return SparkSession.builder.appName("SpotifyDataTransform").getOrCreate()

def load_and_clean(spark, input_dir, output_dir):
    """Stage 1: Load extracted data, perform cleaning and write as parquet."""
    # Define schemas
    artists_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True),
        T.StructField("followers", T.IntegerType(), True),
        T.StructField("genres", T.StringType(), True)
    ])
    
    recommendations_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("related_ids", T.ArrayType(T.StringType()), True)
    ])
    
    tracks_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True),
        T.StructField("duration_ms", T.IntegerType(), True),
        T.StructField("explicit", T.IntegerType(), True),
        T.StructField("artists", T.StringType(), True),
        T.StructField("id_artists", T.StringType(), True),
        T.StructField("release_date", T.StringType(), True),
        T.StructField("danceability", T.FloatType(), True),
        T.StructField("energy", T.FloatType(), True),
        T.StructField("key", T.IntegerType(), True),
        T.StructField("loudness", T.FloatType(), True),
        T.StructField("mode", T.IntegerType(), True),
        T.StructField("speechiness", T.FloatType(), True),
        T.StructField("acousticness", T.FloatType(), True),
        T.StructField("instrumentalness", T.FloatType(), True),
        T.StructField("liveness", T.FloatType(), True),
        T.StructField("valence", T.FloatType(), True),
        T.StructField("tempo", T.FloatType(), True),
        T.StructField("time_signature", T.IntegerType(), True)
    ])
    
    # Load data
    artists_df = spark.read.schema(artists_schema).csv(os.path.join(input_dir, "artists.csv"), header=True)
    recommendations_df = spark.read.schema(recommendations_schema).json(os.path.join(input_dir, "fixed_da.json"))
    tracks_df = spark.read.schema(tracks_schema).csv(os.path.join(input_dir, "tracks.csv"), header=True)
    
    # Clean data
    artists_df = artists_df.dropDuplicates(['id']).filter(F.col('id').isNotNull())
    recommendations_df = recommendations_df.dropDuplicates(['id']).filter(F.col('id').isNotNull())
    tracks_df = tracks_df.dropDuplicates(['id']).filter(F.col('id').isNotNull())
    
    # Write cleaned data
    artists_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "artists"))
    recommendations_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "recommendations"))
    tracks_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "tracks"))
    
    print("Stage 1: Cleaned data saved")
    return artists_df, recommendations_df, tracks_df

def create_master_table(output_dir, artists_df, recommendations_df, tracks_df):
    """Stage 2: Create master table."""
    # Convert id_artists column to array and explode
    tracks_exploded = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists"), T.ArrayType(T.StringType()))) \
                              .withColumn("artist_id", F.explode("id_artists_array")) \
                              .select("id", "name", "popularity", "artist_id")
    
    # Join tracks with artists
    master_df = tracks_exploded.join(artists_df, tracks_exploded.artist_id == artists_df.id, "left") \
        .select(
            tracks_exploded.id.alias("track_id"),
            tracks_exploded.name.alias("track_name"),
            tracks_exploded.popularity.alias("track_popularity"),
            artists_df.id.alias("artist_id"),
            artists_df.name.alias("artist_name"),
            artists_df.followers,
            artists_df.genres,
            artists_df.popularity.alias("artist_popularity")
        )
    
    # Join with recommendations
    master_df = master_df.join(recommendations_df, master_df.artist_id == recommendations_df.id, "left") \
        .select(
            master_df.track_id,
            master_df.track_name,
            master_df.track_popularity,
            master_df.artist_id,
            master_df.artist_name,
            master_df.followers,
            master_df.genres,
            master_df.artist_popularity,
            recommendations_df.related_ids
        )
    
    # Save master table
    master_df.write.mode('overwrite').parquet(os.path.join(output_dir, "stage2", "master_table"))
    print("Stage 2: Master table saved")

def create_query_tables(output_dir, artists_df, recommendations_df, tracks_df):
    """Stage 3: Create query-optimized tables."""
    # Explode recommendations
    recommendations_exploded = recommendations_df.withColumn("related_id", F.explode("related_ids")) \
                                               .select("id", "related_id")
    recommendations_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "recommendations_exploded"))
    
    # Create artist-track mapping
    tracks_exploded = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists"), T.ArrayType(T.StringType()))) \
                              .withColumn("artist_id", F.explode("id_artists_array")) \
                              .select("id", "artist_id")
    tracks_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_track"))
    
    # Create metadata tables
    tracks_metadata = tracks_df.select(
        "id", "name", "popularity", "duration_ms", "danceability", "energy", "tempo"
    )
    tracks_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "track_metadata"))
    
    artists_metadata = artists_df.select("id", "name", "followers", "popularity")
    artists_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_metadata"))
    
    print("Stage 3: Query-optimized tables saved")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    
    spark = create_spark_session()
    
    artists_df, recommendations_df, tracks_df = load_and_clean(spark, input_dir, output_dir)
    create_master_table(output_dir, artists_df, recommendations_df, tracks_df)
    create_query_tables(output_dir, artists_df, recommendations_df, tracks_df)
    
    print("Transformation pipeline completed")


    def create_spark_session():
        """Initialize Spaek session"""
        return (SparkSession.builder
                .appName("SpotifyDataTransform")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate()
        )