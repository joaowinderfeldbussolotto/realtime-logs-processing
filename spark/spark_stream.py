import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.logs (
        id UUID PRIMARY KEY,
        timestamp TEXT,
        service_type TEXT,
        instance_name TEXT,
        severity TEXT,
        region TEXT,
        metrics TEXT,
        cloud_provider TEXT,
        message TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    log_id = kwargs.get('id')
    timestamp = kwargs.get('timestamp')
    service_type = kwargs.get('service_type')
    instance_name = kwargs.get('instance_name')
    severity = kwargs.get('severity')
    region = kwargs.get('region')
    metrics = kwargs.get('metrics')
    cloud_provider = kwargs.get('cloud_provider')
    message = kwargs.get('message')

    try:
        session.execute("""
            INSERT INTO spark_streams.logs(id, timestamp, service_type, instance_name, severity, 
                region, metrics, cloud_provider, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (log_id, timestamp, service_type, instance_name, severity,
              region, metrics, cloud_provider, message))
        logging.info(f"Data inserted for log id {log_id}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .config('spark.cassandra.connection.port', '9042') \
            .config('spark.streaming.stopGracefullyOnShutdown', 'true') \
            .config('spark.sql.shuffle.partitions', '1') \
            .config('spark.streaming.backpressure.enabled', 'true') \
            .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        raise e

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # Add topic existence check
        from kafka.admin import KafkaAdminClient
        admin_client = KafkaAdminClient(bootstrap_servers=['broker:29092'])
        topics = admin_client.list_topics()
        if 'logs' not in topics:
            logging.error("Topic 'logs' does not exist")
            return None
            
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'logs') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.error(f"kafka dataframe could not be created because: {e}")
        raise e

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("service_type", StringType(), False),
        StructField("instance_name", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("region", StringType(), False),
        StructField("metrics", StringType(), False),
        StructField("cloud_provider", StringType(), False),
        StructField("message", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    try:
        spark_conn = create_spark_connection()

        if spark_conn is not None:
            # connect to kafka with spark connection
            spark_df = connect_to_kafka(spark_conn)
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session is not None:
                create_keyspace(session)
                create_table(session)

                logging.info("Streaming is being started...")

                streaming_query = (selection_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'logs')
                                   .trigger(processingTime='3 seconds')
                                   .option('spark.cassandra.connection.host', 'cassandra')
                                   .option('spark.cassandra.connection.port', '9042')
                                   .start())

                streaming_query.awaitTermination()
            else:
                logging.error("Could not create Cassandra session")
        else:
            logging.error("Could not create Spark connection")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise e