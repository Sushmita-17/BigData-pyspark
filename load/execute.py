import sys
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

def create_spark_session():
    """Initialize Spark session with PostgresQL JDBC driver"""
    return SparkSession.builder \
        .appName("SpotifyDataLoad") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()

def create_postgres_tables():
    """Create PostgreSQL tables if they don't exist"""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Sushmita",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        create_table_queries = [
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id VARCHAR(50),
                artist_name TEXT,
                followers FLOAT,
                genres TEXT,
                artist_popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT,
                related_ids TEXT[]
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
                id VARCHAR(50),
                related_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_track (
                track_id VARCHAR(50),
                artist_id VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS track_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                popularity INTEGER,
                duration_ms INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS artist_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                followers FLOAT,
                popularity INTEGER
            );
            """
        ]
        for query in create_table_queries:
            cursor.execute(query)
            conn.commit()
            print("PostgreSQL tables created successfully")

    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir):
    """Load Parquet files to PostgreSQL"""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "Sushmita",
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table", "overwrite"),
        ("stage3/recommendations_exploded", "recommendations_exploded", "overwrite"),
        ("stage3/artist_track", "artist_track", "overwrite"),
        ("stage3/track_metadata", "track_metadata", "overwrite"),
        ("stage3/artist_metadata", "artist_metadata", "overwrite")
    ]
    
    for parquet_path, table_name, mode in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            df.write \
                .mode(mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            print(f"Successfully loaded {table_name}")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load/execute.py <input_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    if not os.path.exists(input_dir):
        print(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)

    spark = create_spark_session()
    create_postgres_tables()
    load_to_postgres(spark, input_dir)
    spark.stop()
    print("Load stage completed")