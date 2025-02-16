#### Spark Streaming Application Overview

This directory contains the Spark Streaming application responsible for consuming log data from Kafka and storing it in Cassandra.

**Functionality:**

*   Connects to a Kafka topic (`logs`).
*   Reads log data from Kafka in real-time.
*   Transforms the data using Spark SQL.
*   Stores the processed data into a Cassandra database.

**Dependencies:**

*   Spark cluster
*   Kafka broker
*   Cassandra database
*   `spark-cassandra-connector` and `spark-sql-kafka` dependencies.

**How to Use:**

1.  Ensure that the Spark cluster, Kafka broker, and Cassandra database are running and accessible.
2.  Configure the Spark application with the correct Kafka and Cassandra connection details.
3.  Submit the Spark application to the cluster.

**Configuration:**

The following configurations are used in the Spark application:

*   `spark.jars.packages`: Specifies the required dependencies for Kafka and Cassandra integration.
*   `spark.cassandra.connection.host`: Cassandra host address.
*   `spark.cassandra.connection.port`: Cassandra port.
*   `spark.streaming.stopGracefullyOnShutdown`: Enables graceful shutdown of the streaming application.
*   `spark.sql.shuffle.partitions`: Sets the number of partitions for shuffling data.
*   `spark.streaming.backpressure.enabled`: Enables backpressure to handle varying data ingestion rates.
*   `spark.sql.streaming.forceDeleteTempCheckpointLocation`: Forces deletion of the checkpoint location on startup.

**Notes:**

*   The application uses a checkpoint location (`/tmp/checkpoint`) to store the state of the streaming job.
*   The application creates a Cassandra keyspace (`spark_streams`) and table (`logs`) if they do not exist.
