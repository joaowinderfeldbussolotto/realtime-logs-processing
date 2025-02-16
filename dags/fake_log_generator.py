import uuid
from datetime import datetime
import random
import pytz

AWS_SERVICES = {
    'ec2': ['web-server-prod', 'app-server-prod', 'worker-node'],
    'rds': ['postgres-main', 'mysql-replica', 'aurora-cluster'],
    'lambda': ['etl-function', 'api-handler', 'data-processor'],
    's3': ['data-lake-bucket', 'backup-bucket', 'archive-store']
}

GCP_SERVICES = {
    'compute_engine': ['vm-backend', 'vm-frontend', 'batch-processor'],
    'big_query': ['media-bucket', 'logs-bucket', 'backup-store']
}

def get_service_metrics(service_type):
    base_metrics = {
        'ec2': {
            'cpu': random.uniform(0, 100),
            'memory': random.uniform(0, 100)
        },
        'rds': {
            'connections': random.randint(0, 1000),
            'latency': random.uniform(0, 100)
        },
        'lambda': {
            'duration': random.uniform(0, 1000),
            'memory': random.uniform(0, 128)
        },
        'compute_engine': {
            'cpu': random.uniform(0, 100),
            'memory': random.uniform(0, 100)
        },
        'cloud_storage': {
            'objects': random.randint(0, 100000),
            'size': random.uniform(0, 1000)
        },
        's3': {
            'objects': random.randint(0, 100000),
            'size': random.uniform(0, 1000)
        }
    }
    return base_metrics.get(service_type, {'generic': random.uniform(0, 100)})

def safe_metric_format(metrics, key, format_spec='.1f'):
    try:
        value = metrics.get(key, 0)
        return format(value, format_spec)
    except (ValueError, TypeError):
        return '0'

def generate_log_message(service_type, instance_name, metrics, severity):
    timestamp = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    trace_id = request_id = str(uuid.uuid4())[:8]

    messages = {
        'ecs': {
            'INFO': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.task.status cluster=prod-cluster service=order-service taskId=12345678-1234-1234-1234-123456789012 - Task started successfully. Container: order-processor:v1.2.3, CPU: {safe_metric_format(metrics, 'cpu')}%, Memory: {safe_metric_format(metrics, 'memory')}MB/{safe_metric_format(metrics, 'memory_limit')}MB",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.service.scaling cluster=prod-cluster service=payment-service - Service scaled successfully. Desired count: 4, Running count: 4, Pending count: 0. Target CPU utilization: 75%, Current: {safe_metric_format(metrics, 'service_cpu')}%",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.container.health cluster=prod-cluster service=user-service taskId=98765432-9876-9876-9876-987654321098 - Health check passed. Status: healthy, Response time: {safe_metric_format(metrics, 'health_response_time')}ms, Container IP: 172.31.16.8"
            ],
            'WARNING': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.container.memory cluster=prod-cluster service=cache-service taskId=abcdef12-abcd-abcd-abcd-abcdef123456 - High memory usage: {safe_metric_format(metrics, 'memory')}MB/{safe_metric_format(metrics, 'memory_limit')}MB (95%). OOM risk. Top processes: [redis-server: {metrics.get('redis_memory')}MB], [node: {metrics.get('node_memory')}MB]",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.service.unstable cluster=prod-cluster service=auth-service - Service unstable: {metrics.get('container_exits')} container exits in 5 minutes. Exit codes: [137: {metrics.get('oom_exits')} times], [1: {metrics.get('error_exits')} times]. Last log: 'java.lang.OutOfMemoryError: Java heap space'",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.networking.latency cluster=prod-cluster service=api-gateway taskId=45678901-4567-4567-4567-456789012345 - High network latency: {safe_metric_format(metrics, 'network_latency')}ms to RDS. Packet loss: {safe_metric_format(metrics, 'packet_loss')}%. Route table: rtb-01234567"
            ],
            'ERROR': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.task.failure cluster=prod-cluster service=order-service taskId=23456789-2345-2345-2345-234567890123 - Task failed to start. Error: 'CannotPullContainerError: Error response from daemon: pull access denied for order-processor:v1.2.3, repository does not exist or may require docker login'. Registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.volume.error cluster=prod-cluster service=data-processor taskId=34567890-3456-3456-3456-345678901234 - EFS mount failed. Volume: fs-0abc123def456, Mount target: fsmt-12345678, Error: 'mount.nfs4: Connection reset by peer'. Host mount point: /mnt/efs",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] ecs.service.crash cluster=prod-cluster service=payment-processor taskId=56789012-5678-5678-5678-567890123456 - Service crashed. Container exited with code 139 (Segmentation fault). Core dump saved at /tmp/core.1234. Last log line: 'Signal: SIGSEGV (11) @ 0x7f8b4c3d2a40, fault addr: 0x0'"
            ]
        },
        'rds': {
            'INFO': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.instance.metrics instance=prod-db-1 - Performance metrics: IOPS={metrics.get('iops')}, Connections={metrics.get('connections')}/{metrics.get('max_connections')}, Queries/sec={safe_metric_format(metrics, 'queries_per_second')}, Buffer cache hit ratio={safe_metric_format(metrics, 'buffer_hit_ratio')}%",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.backup.success instance=prod-db-1 - Automated snapshot completed successfully. Snapshot: rds:prod-db-1-2025-02-15-01-00, Size: {safe_metric_format(metrics, 'backup_size')}GB, Duration: {metrics.get('backup_duration')}s",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.maintenance.completed instance=prod-db-1 - Maintenance window tasks completed. Applied patch: MySQL-8.0.35.123, Previous version: 8.0.35.111. Downtime: {metrics.get('maintenance_duration')}s"
            ],
            'WARNING': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.storage.warning instance=prod-db-1 - Storage space low: {safe_metric_format(metrics, 'storage_used')}GB/{safe_metric_format(metrics, 'storage_total')}GB (90%). Largest tables: orders ({safe_metric_format(metrics, 'orders_size')}GB), audit_logs ({safe_metric_format(metrics, 'audit_size')}GB)",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.performance.degraded instance=prod-db-1 - Query performance degraded. Long-running query detected (duration: {safe_metric_format(metrics, 'query_duration')}s): SELECT * FROM orders WHERE created_at > '2025-01-01' AND status = 'pending' ORDER BY total_amount DESC. No index used.",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.replication.lag instance=prod-db-1 - Replication lag increasing: {safe_metric_format(metrics, 'replication_lag')}s. Master: prod-db-master. Binary log: mysql-bin-changelog.123456, Position: {metrics.get('binlog_position')}"
            ],
            'ERROR': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.instance.crash instance=prod-db-1 - Database crash detected. Error in mysql-error.log: 'InnoDB: Table `orders`.`idx_created_at` is corrupted. Row in wrong partition: \"created_at\" value is less than maxvalue for previous partition'. Thread ID: {metrics.get('thread_id')}",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.backup.failure instance=prod-db-1 - Automated backup failed. Error: 'Cannot create backup because binary logging is not enabled'. Task ID: {metrics.get('task_id')}, Status: failed, Impact: RPO compliance at risk",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] rds.storage.failure instance=prod-db-1 - Storage subsystem failure. Error: 'EBS volume vol-0abc123def456789 is unresponsive'. IO errors detected, database in read-only mode. Affected tables: users, orders, products"
            ]
        },
        'lambda': {
            'INFO': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.execution.start function=order-processor version=$LATEST - Cold start completed in {metrics.get('init_duration')}ms. Memory configured: {metrics.get('memory_size')}MB, Runtime: Node.js 18.x",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.execution.success function=payment-handler version=12 - Execution completed successfully. Duration: {metrics.get('duration')}ms, Memory used: {safe_metric_format(metrics, 'memory_used')}MB, Billed duration: {metrics.get('billed_duration')}ms",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.scaling.event function=image-processor version=$LATEST - Concurrent executions: {metrics.get('concurrent_executions')}, Reserved concurrency: {metrics.get('reserved_concurrency')}"
            ],
            'WARNING': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.timeout.risk function=data-processor version=5 - Function approaching timeout. Current execution: {safe_metric_format(metrics, 'execution_time')}ms of {metrics.get('timeout')}ms limit. Memory usage: {safe_metric_format(metrics, 'memory_used')}MB",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.memory.high function=video-encoder version=$LATEST - High memory usage: {safe_metric_format(metrics, 'memory_used')}MB of {metrics.get('memory_size')}MB allocated. Consider increasing memory allocation",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.throttling.detected function=notification-sender version=3 - Throttling detected. Concurrent executions: {metrics.get('concurrent_executions')}/{metrics.get('account_limit')}. Reserved concurrency: {metrics.get('reserved_concurrency')}"
            ],
            'ERROR': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.execution.failed function=order-processor version=$LATEST - Uncaught exception: 'TypeError: Cannot read property 'Items' of undefined'. Stack trace: at /var/task/index.js:123:45. Request ID: {request_id}",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.timeout.exceeded function=data-processor version=5 - Function timed out after {metrics.get('timeout')}ms. Last log: 'Processing batch 45 of 100'. Memory usage at timeout: {safe_metric_format(metrics, 'memory_used')}MB",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] lambda.permission.denied function=s3-processor version=$LATEST - IAM permission denied: 's3:PutObject' on resource 'arn:aws:s3:::prod-data/*'. Principal: {metrics.get('principal_id')}, Account: {metrics.get('account_id')}"
            ]
        },
        's3': {
            'INFO': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.object.uploaded bucket=prod-data key=uploads/2025/02/15/data.parquet - Upload completed. Size: {safe_metric_format(metrics, 'object_size')}MB, Storage class: STANDARD, Server-side encryption: AES256",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.bucket.metrics bucket=prod-assets - Daily metrics: Objects: {metrics.get('object_count')}, Total size: {safe_metric_format(metrics, 'total_size')}TB, Requests: {metrics.get('request_count')}, Bandwidth: {safe_metric_format(metrics, 'bandwidth')}GB",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.replication.status bucket=prod-backup - Cross-region replication healthy. Destination: us-west-2, Pending objects: {metrics.get('pending_objects')}, Replication latency: {safe_metric_format(metrics, 'replication_latency')}s"
            ],
            'WARNING': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.bucket.size bucket=prod-logs - Bucket size growing rapidly: {safe_metric_format(metrics, 'growth_rate')}GB/day. Current size: {safe_metric_format(metrics, 'current_size')}TB. Consider implementing lifecycle rules",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.access.unusual bucket=prod-data - Unusual access pattern detected. IPs: {metrics.get('unusual_ips')}, Request rate: {safe_metric_format(metrics, 'request_rate')}/s (500% above baseline)",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.versioning.limit bucket=prod-assets key=images/logo.png - Object version count high: {metrics.get('version_count')} versions. Size of versions: {safe_metric_format(metrics, 'versions_size')}GB"
            ],
            'ERROR': [
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.replication.failed bucket=prod-backup key=daily/backup-2025-02-14.zip - Replication failed to destination bucket. Error: 'KMS.NotFoundException: Key arn:aws:kms:us-west-2:123456789012:key/abcd1234-a123-456a-a12b-a123b4cd5678 does not exist'",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.corruption.detected bucket=prod-data key=database/dump.sql - Object corruption detected. ETag mismatch: expected {metrics.get('expected_etag')}, got {metrics.get('actual_etag')}. Upload incomplete or data corrupted",
                f"{timestamp} {severity} [{request_id}] [TraceID: {trace_id}] s3.deletion.error bucket=prod-assets - DeleteObjects operation failed. Error: 'AccessDenied: Access Denied for object deletion'. Affected objects: {metrics.get('affected_objects')}, Principal: {metrics.get('principal_id')}"
            ]
        },
        'compute_engine': {
            'INFO': [
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.instance.metrics instance=prod-vm-1 - CPU: {safe_metric_format(metrics, 'cpu')}%, Memory: {safe_metric_format(metrics, 'memory')}%, Disk IO: {safe_metric_format(metrics, 'disk_io')}MB/s, Network: {safe_metric_format(metrics, 'network_throughput')}Mbps",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.disk.operations instance=prod-vm-1 - SSD persistent disk operations. Read ops: {metrics.get('read_ops')}/s, Write ops: {metrics.get('write_ops')}/s, IO queue length: {metrics.get('io_queue')}",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.load.balancing instance=prod-lb-1 - Health check passed. Backend instances healthy: {metrics.get('healthy_instances')}, Total instances: {metrics.get('total_instances')}, Traffic distribution: {safe_metric_format(metrics, 'traffic_distribution')}%"
            ],
            'WARNING': [
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.quota.approaching project=my-project - Approaching CPU quota limit: {metrics.get('cpu_usage')}/24 vCPUs used in us-central1. 85% of quota reached. N1 Machine type quota nearly exhausted",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.disk.performance instance=prod-vm-2 - High disk latency detected: {safe_metric_format(metrics, 'disk_latency')}ms average. Disk type: pd-ssd, Operation type: write, Queue depth: {metrics.get('queue_depth')}",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.network.congestion instance=prod-vm-3 - Network performance degraded. Packet loss: {safe_metric_format(metrics, 'packet_loss')}%, Latency: {safe_metric_format(metrics, 'network_latency')}ms, Retransmission rate: {safe_metric_format(metrics, 'retransmission_rate')}%"
            ],
            'ERROR': [
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.instance.crash instance=prod-vm-4 - System crash detected. Kernel panic at address 0xFFFF8801C3A7B000. Stack trace available. Last message: 'Unable to handle kernel paging request'. Machine state: Non-responsive",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.disk.failure instance=prod-vm-5 - Persistent disk failure. Device: /dev/sda1, Serial: 0x123456789. Multiple sector errors detected. SMART Status: FAILING_NOW. Reallocated sector count: {metrics.get('reallocated_sectors')}",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] gce.network.outage instance=prod-vm-6 - Network interface failure. Interface: eth0, Driver: virtio-net, TX errors: {metrics.get('tx_errors')}, RX errors: {metrics.get('rx_errors')}, Carrier changes: {metrics.get('carrier_changes')}"
            ]
        },
        'big_query': {
            'INFO': [
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.query.execution project=my-project job_id=job_xyz123 - Query completed successfully. Bytes processed: {safe_metric_format(metrics, 'bytes_processed')}TB, Slot time consumed: {metrics.get('slot_ms')}ms, Cache hit: {metrics.get('cache_hit')}",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.table.load dataset=analytics table=daily_metrics - Table load completed. Rows inserted: {metrics.get('rows_inserted')}, Bytes processed: {safe_metric_format(metrics, 'bytes_loaded')}GB, Source format: PARQUET",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.streaming.metrics dataset=events table=user_actions - Streaming insert metrics: {metrics.get('rows_streamed')} rows, Latency: {safe_metric_format(metrics, 'streaming_latency')}ms, Success rate: {safe_metric_format(metrics, 'success_rate')}%"
            ],
            'WARNING': [
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.query.performance project=my-project job_id=job_abc456 - Query performance warning. Table scan too large: {safe_metric_format(metrics, 'bytes_scanned')}TB. Consider partitioning table 'production.user_events'. Estimated cost: ${safe_metric_format(metrics, 'estimated_cost')}",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.quota.usage project=my-project - Approaching query quota limit. Daily usage: {safe_metric_format(metrics, 'daily_usage')}TB/{metrics.get('quota_tb')}TB. Consider implementing cost controls",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.streaming.bottleneck dataset=realtime table=clicks - Streaming insert latency high. Current latency: {safe_metric_format(metrics, 'current_latency')}ms, Rows buffered: {metrics.get('buffered_rows')}, Failed insertions: {metrics.get('failed_insertions')}"
            ],
            'ERROR': [
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.query.failed project=my-project job_id=job_def789 - Query failed. Error: 'Resources exceeded during query execution: Out of memory error in JOIN operation'. Memory used: {safe_metric_format(metrics, 'memory_used')}GB, Query size: {safe_metric_format(metrics, 'query_size')}MB",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.data.corruption dataset=finance table=transactions - Data corruption detected during table load. Error parsing row {metrics.get('error_row')}: 'Invalid JSON payload received. Unknown name transaction_date. Job ID: bq_job_123",
                f"2025-02-15 01:54:22 {severity} [{request_id}] [TraceID: {trace_id}] bq.permission.denied project=my-project dataset=confidential - Access denied to table 'confidential.salary_data'. Required permission: 'bigquery.tables.get'. Principal: service-account-xyz@my-project.iam, Resource: projects/my-project/datasets/confidential/tables/salary_data"
            ]
        }
    }

    service_messages = messages.get(service_type)
    if service_messages:
        return random.choice(service_messages.get(severity, service_messages['INFO']))
    else:
        return f"{timestamp} {severity} [{request_id}] {service_type}:{instance_name} - Standard monitoring check completed"

def generate_log_data(service_type, instance_name):
    current_time = datetime.now(pytz.UTC)
    severity = random.choices(
        ['INFO', 'WARNING', 'ERROR', 'DEBUG'],
        weights=[0.5, 0.1, 0.1, 0.3],
        k=1
    )[0]
    
    metrics_data = get_service_metrics(service_type)
    
    log_data = {
        'id': str(uuid.uuid4()),
        'timestamp': current_time.isoformat(),
        'service_type': service_type,
        'instance_name': instance_name,
        'severity': severity,
        'region': random.choice(['us-east-1', 'us-west-2', 'eu-west-1']),
        'metrics': metrics_data,
        'cloud_provider': 'AWS' if service_type in AWS_SERVICES else 'GCP',
        'message': generate_log_message(service_type, instance_name, metrics_data, severity)
    }
    return log_data
