#### Log Analyzer Module Overview

This module is responsible for consuming log messages from Kafka, analyzing error logs using Mistral AI, and sending notifications to Slack.

**Functionality:**

*   Connects to a Kafka topic (`logs`).
*   Consumes log messages in batches.
*   Filters for error messages.
*   Analyzes error messages using the Mistral AI API.
*   Formats the analysis and log data into a Slack message.
*   Sends the Slack message to a specified channel.

**Dependencies:**

*   Kafka broker
*   Mistral AI API key
*   Slack bot token
*   `slack_sdk`, `kafka-python`, `requests` libraries.

**How to Use:**

1.  Ensure that the Kafka broker is running and accessible.
2.  Set the `SLACK_BOT_TOKEN` and `MISTRAL_API_KEY` environment variables.
3.  Configure the Kafka connection details in `batch_consumer.py`.
4.  Run the `batch_consumer.py` script.

**Configuration:**

The following configurations are available:

*   `batch_size`: The number of messages to process in each batch (default: 10).
*   `max_wait_seconds`: The maximum time to wait for a batch to fill up (default: 30 seconds).
*   `max_retries`: The maximum number of retries for Kafka connection (default: 5).
*   `retry_delay`: The delay between retries in seconds (default: 5).
*   `channel`: The Slack channel to send notifications to (default: `#it`).

**Files:**

*   `batch_consumer.py`: Main script for consuming messages, analyzing errors, and sending Slack notifications.
*   `slack_utils.py`: Utility class for sending Slack notifications.
*   `mistral_utils.py`: Utility class for interacting with the Mistral AI API.
*   `message_formatter.py`: Function for formatting Slack message blocks.
*   `requirements.txt`: Lists the required Python packages.
*   `Dockerfile`: Dockerfile for containerizing the application.
