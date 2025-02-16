#### DAGs Overview

This directory contains Airflow DAGs for the real-time logs processing pipeline. 

**Current DAGs:**

*   **log\_streaming:**  This DAG is responsible for streaming log data to a Kafka topic. It uses a PythonOperator to execute a function that generates fake log data and sends it to Kafka.

**Dependencies:**

*   Kafka broker
*   `fake_log_generator.py` for generating log data.

**How to Use:**

1.  Ensure that the Kafka broker is running and accessible.
2.  Place the DAG files in Airflow's DAGs folder.
3.  Configure the necessary connections and variables in Airflow.
4.  Enable the DAGs in the Airflow UI.

