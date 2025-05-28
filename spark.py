# ============================================================================
# SPARK STRUCTURED STREAMING WITH PYTHON - COMPREHENSIVE GUIDE
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
import json
from datetime import datetime, timedelta

# ============================================================================
# 1. BASIC STREAMING SETUP
# ============================================================================

def create_spark_session():
    """Create Spark session with streaming configurations"""
    return SparkSession.builder \
        .appName("StreamProcessing") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

# ============================================================================
# 2. FILE STREAMING - MONITORING DIRECTORIES
# ============================================================================

def file_stream_processing():
    """Process files as they arrive in a directory"""
    
    spark = create_spark_session()
    
    # Define schema for incoming JSON files
    json_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("session_id", StringType(), True)
    ])
    
    # Read streaming data from files
    df = spark.readStream \
        .option("maxFilesPerTrigger", 10) \
        .schema(json_schema) \
        .json("/data/streaming/input")
    
    # Add processing timestamp
    processed_df = df.withColumn("processing_time", current_timestamp())
    
    # Basic transformations
    enriched_df = processed_df \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("date", to_date(col("timestamp"))) \
        .filter(col("amount") > 0) \
        .filter(col("event_type").isin(["purchase", "add_to_cart", "view"]))
    
    # Write to console for debugging
    query = enriched_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query

# ============================================================================
# 3. KAFKA STREAMING - REAL-TIME MESSAGE PROCESSING
# ============================================================================

def kafka_stream_processing():
    """Process real-time data from Kafka"""
    
    spark = create_spark_session()
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user_events,transactions,clicks") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Define schema for the JSON messages
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("topic", "partition", "offset", "kafka_timestamp", "data.*")
    
    # Convert timestamp and add derived fields
    processed_df = parsed_df \
        .withColumn("event_time", from_unixtime(col("timestamp")/1000).cast(TimestampType())) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("delay_seconds", 
                   unix_timestamp(col("processing_time")) - col("timestamp")/1000) \
        .withColumn("hour", hour(col("event_time"))) \
        .withColumn("day_of_week", dayofweek(col("event_time")))
    
    return processed_df

# ============================================================================
# 4. WINDOW OPERATIONS AND AGGREGATIONS
# ============================================================================

def windowed_aggregations():
    """Perform windowed operations on streaming data"""
    
    spark = create_spark_session()
    
    # Get streaming data (using Kafka example)
    streaming_df = kafka_stream_processing()
    
    # Tumbling window aggregations (non-overlapping windows)
    tumbling_window = streaming_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("event_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            avg("delay_seconds").alias("avg_delay"),
            max("delay_seconds").alias("max_delay")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("event_count"),
            col("unique_users"),
            col("avg_delay"),
            col("max_delay")
        )
    
    # Sliding window aggregations (overlapping windows)
    sliding_window = streaming_df \
        .withWatermark("event_time", "15 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "2 minutes"),  # 10min window, slide every 2min
            col("user_id")
        ) \
        .agg(
            count("*").alias("user_events"),
            collect_list("event_type").alias("event_sequence"),
            first("event_time").alias("first_event"),
            last("event_time").alias("last_event")
        )
    
    # Session-based windows (custom logic)
    session_windows = streaming_df \
        .withWatermark("event_time", "30 minutes") \
        .groupBy("user_id", "session_id") \
        .agg(
            min("event_time").alias("session_start"),
            max("event_time").alias("session_end"),
            count("*").alias("session_events"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            collect_set("product_id").alias("viewed_products")
        ) \
        .withColumn("session_duration", 
                   unix_timestamp(col("session_end")) - unix_timestamp(col("session_start")))
    
    return tumbling_window, sliding_window, session_windows

# ============================================================================
# 5. COMPLEX EVENT PROCESSING AND PATTERN DETECTION
# ============================================================================

def complex_event_processing():
    """Detect complex patterns in streaming data"""
    
    spark = create_spark_session()
    streaming_df = kafka_stream_processing()
    
    # Fraud detection: Multiple high-value transactions in short time
    fraud_detection = streaming_df \
        .withWatermark("event_time", "5 minutes") \
        .filter(col("event_type") == "purchase") \
        .groupBy(
            col("user_id"),
            window(col("event_time"), "2 minutes")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("properties.amount").alias("total_amount"),
            collect_list("properties.amount").alias("amounts")
        ) \
        .filter(
            (col("transaction_count") > 5) | 
            (col("total_amount") > 10000)
        ) \
        .withColumn("alert_type", 
                   when(col("transaction_count") > 5, "high_frequency")
                   .when(col("total_amount") > 10000, "high_value")
                   .otherwise("suspicious")) \
        .withColumn("risk_score", 
                   col("transaction_count") * 10 + col("total_amount") / 1000)
    
    # User journey analysis: Cart abandonment detection
    cart_analysis = streaming_df \
        .withWatermark("event_time", "1 hour") \
        .groupBy("user_id", "session_id") \
        .agg(
            collect_list(
                struct(col("event_time"), col("event_type"), col("product_id"))
            ).alias("events")
        ) \
        .withColumn("has_add_to_cart", 
                   array_contains(col("events.event_type"), "add_to_cart")) \
        .withColumn("has_purchase", 
                   array_contains(col("events.event_type"), "purchase")) \
        .filter(col("has_add_to_cart") & ~col("has_purchase")) \
        .withColumn("abandoned_at", current_timestamp())
    
    return fraud_detection, cart_analysis

# ============================================================================
# 6. STATE MANAGEMENT AND STATEFUL OPERATIONS
# ============================================================================

from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

def stateful_processing():
    """Implement stateful stream processing"""
    
    spark = create_spark_session()
    streaming_df = kafka_stream_processing()
    
    # Define state structure
    class UserSessionState:
        def __init__(self):
            self.session_start = None
            self.last_activity = None
            self.event_count = 0
            self.total_value = 0.0
            self.events = []
    
    # Stateful operation using mapGroupsWithState
    def update_user_session(key, events, state: GroupState):
        """Update user session state"""
        
        if state.hasTimedOut:
            # Session timed out, emit final result
            user_state = state.get
            result = {
                'user_id': key,
                'session_duration': (user_state.last_activity - user_state.session_start).total_seconds(),
                'total_events': user_state.event_count,
                'total_value': user_state.total_value,
                'session_end_reason': 'timeout'
            }
            state.remove()
            return result
        
        # Process new events
        if state.exists:
            user_state = state.get
        else:
            user_state = UserSessionState()
        
        for event in events:
            if user_state.session_start is None:
                user_state.session_start = event.event_time
            
            user_state.last_activity = event.event_time
            user_state.event_count += 1
            
            if hasattr(event, 'amount') and event.amount:
                user_state.total_value += event.amount
            
            user_state.events.append({
                'event_type': event.event_type,
                'timestamp': event.event_time
            })
        
        # Set timeout for 30 minutes of inactivity
        state.setTimeoutDuration("30 minutes")
        state.update(user_state)
        
        return None  # Don't emit intermediate results
    
    # Apply stateful operation
    stateful_df = streaming_df \
        .groupByKey(lambda x: x.user_id) \
        .mapGroupsWithState(
            update_user_session,
            outputMode=OutputMode.Update(),
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
        )
    
    return stateful_df

# ============================================================================
# 7. MULTIPLE OUTPUT SINKS
# ============================================================================

def multiple_output_sinks():
    """Write streaming data to multiple destinations"""
    
    spark = create_spark_session()
    
    # Source data
    streaming_df = kafka_stream_processing()
    
    # Different processing paths
    real_time_alerts = streaming_df \
        .filter(col("event_type") == "error") \
        .select("event_time", "user_id", "event_type", "properties")
    
    analytics_data = streaming_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("event_type")
        ) \
        .count()
    
    user_activity = streaming_df \
        .select("user_id", "event_time", "event_type") \
        .withColumn("date", to_date(col("event_time")))
    
    # Output to Kafka (for downstream systems)
    kafka_query = real_time_alerts \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "alerts") \
        .option("checkpointLocation", "/tmp/kafka_checkpoint") \
        .outputMode("append") \
        .start()
    
    # Output to Delta Lake (for analytics)
    delta_query = analytics_data \
        .writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", "/tmp/delta_checkpoint") \
        .table("analytics.event_counts")
    
    # Output to Parquet files (partitioned by date)
    parquet_query = user_activity \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "/data/user_activity") \
        .option("checkpointLocation", "/tmp/parquet_checkpoint") \
        .partitionBy("date") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Output to database (PostgreSQL example)
    def write_to_postgres(df, epoch_id):
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/analytics") \
            .option("dbtable", "real_time_metrics") \
            .option("user", "postgres") \
            .option("password", "password") \
            .mode("append") \
            .save()
    
    postgres_query = analytics_data \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .trigger(processingTime="60 seconds") \
        .start()
    
    return [kafka_query, delta_query, parquet_query, postgres_query]

# ============================================================================
# 8. ERROR HANDLING AND MONITORING
# ============================================================================

class StreamingMonitor:
    """Monitor streaming job health and performance"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.metrics = {}
    
    def setup_monitoring(self, query):
        """Setup monitoring for a streaming query"""
        
        def monitor_progress(progress):
            """Monitor query progress"""
            self.metrics[query.name] = {
                'id': progress.id,
                'runId': progress.runId,
                'timestamp': progress.timestamp,
                'batchId': progress.batchId,
                'numInputRows': progress.inputRowsPerSecond,
                'inputRowsPerSecond': progress.inputRowsPerSecond,
                'processedRowsPerSecond': progress.processedRowsPerSecond,
                'durationMs': progress.durationMs,
                'stateOperators': progress.stateOperators,
                'sources': progress.sources,
                'sink': progress.sink
            }
            
            # Log metrics
            print(f"Query {query.name}: {progress.inputRowsPerSecond} rows/sec")
            
            # Check for issues
            if progress.inputRowsPerSecond and progress.inputRowsPerSecond > 10000:
                print(f"WARNING: High input rate for {query.name}")
            
            if progress.durationMs.get('triggerExecution', 0) > 60000:
                print(f"WARNING: Long processing time for {query.name}")
        
        # Add progress listener
        self.spark.streams.addListener(monitor_progress)
        return monitor_progress
    
    def handle_errors(self, df):
        """Add error handling to DataFrame"""
        
        # Add error handling columns
        error_handled_df = df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("is_valid", 
                       when(col("user_id").isNotNull() & 
                            col("event_time").isNotNull(), True)
                       .otherwise(False))
        
        # Split valid and invalid records
        valid_df = error_handled_df.filter(col("is_valid"))
        invalid_df = error_handled_df.filter(~col("is_valid"))
        
        # Log invalid records
        invalid_query = invalid_df \
            .writeStream \
            .format("console") \
            .option("truncate", False) \
            .outputMode("append") \
            .queryName("error_logs") \
            .start()
        
        return valid_df, invalid_query

# ============================================================================
# 9. PRODUCTION DEPLOYMENT EXAMPLE
# ============================================================================

def production_streaming_app():
    """Production-ready streaming application"""
    
    # Configure Spark for production
    spark = SparkSession.builder \
        .appName("ProductionStreamProcessing") \
        .config("spark.sql.streaming.checkpointLocation", "/prod/checkpoints") \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.sql.streaming.ui.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Initialize monitoring
    monitor = StreamingMonitor(spark)
    
    try:
        # Read from Kafka with production settings
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
            .option("subscribe", "events") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 100000) \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .load()
        
        # Parse and validate data
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), 
                     StructType([
                         StructField("user_id", StringType(), True),
                         StructField("event_type", StringType(), True),
                         StructField("timestamp", LongType(), True),
                         StructField("data", MapType(StringType(), StringType()), True)
                     ])).alias("event")
        ).select("event.*") \
         .withColumn("event_time", from_unixtime(col("timestamp")/1000).cast(TimestampType()))
        
        # Add error handling
        valid_df, error_query = monitor.handle_errors(parsed_df)
        
        # Business logic - real-time analytics
        analytics_df = valid_df \
            .withWatermark("event_time", "5 minutes") \
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users")
            )
        
        # Write to multiple sinks with different triggers
        
        # 1. Real-time dashboard (every 10 seconds)
        dashboard_query = analytics_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092") \
            .option("topic", "dashboard_metrics") \
            .option("checkpointLocation", "/prod/checkpoints/dashboard") \
            .outputMode("update") \
            .trigger(processingTime="10 seconds") \
            .queryName("dashboard_stream") \
            .start()
        
        # 2. Data warehouse (every 5 minutes)
        warehouse_query = analytics_df \
            .writeStream \
            .format("delta") \
            .outputMode("complete") \
            .option("checkpointLocation", "/prod/checkpoints/warehouse") \
            .option("path", "/prod/data/analytics") \
            .trigger(processingTime="5 minutes") \
            .queryName("warehouse_stream") \
            .start()
        
        # Setup monitoring for all queries
        monitor.setup_monitoring(dashboard_query)
        monitor.setup_monitoring(warehouse_query)
        monitor.setup_monitoring(error_query)
        
        # Wait for termination
        queries = [dashboard_query, warehouse_query, error_query]
        
        # Graceful shutdown
        import signal
        def signal_handler(signum, frame):
            print("Shutting down gracefully...")
            for query in queries:
                query.stop()
            spark.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Keep running
        for query in queries:
            query.awaitTermination()
    
    except Exception as e:
        print(f"Streaming application failed: {e}")
        spark.stop()
        raise

# ============================================================================
# 10. UTILITY FUNCTIONS AND TESTING
# ============================================================================

def generate_test_data():
    """Generate test data for streaming"""
    
    import random
    import time
    import json
    import os
    
    events = ["view", "add_to_cart", "purchase", "remove_from_cart"]
    users = [f"user_{i}" for i in range(1000)]
    products = [f"product_{i}" for i in range(100)]
    
    output_dir = "/tmp/streaming_test_data"
    os.makedirs(output_dir, exist_ok=True)
    
    while True:
        # Generate batch of events
        batch = []
        for _ in range(random.randint(10, 100)):
            event = {
                "event_id": f"evt_{random.randint(1, 1000000)}",
                "user_id": random.choice(users),
                "event_type": random.choice(events),
                "product_id": random.choice(products),
                "timestamp": int(time.time() * 1000),
                "session_id": f"session_{random.randint(1, 10000)}",
                "amount": random.uniform(10, 1000) if random.random() > 0.7 else None
            }
            batch.append(event)
        
        # Write to file
        filename = f"{output_dir}/events_{int(time.time())}.json"
        with open(filename, 'w') as f:
            for event in batch:
                f.write(json.dumps(event) + '\n')
        
        print(f"Generated {len(batch)} events in {filename}")
        time.sleep(5)  # Generate new file every 5 seconds

def run_simple_example():
    """Run a simple streaming example"""
    
    spark = create_spark_session()
    
    # Create a simple DataFrame from socket stream
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    
    # Split lines into words and count them
    words = lines.select(
        explode(split(lines.value, " ")).alias("word")
    )
    
    word_counts = words.groupBy("word").count()
    
    # Output to console
    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    query.awaitTermination()

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python streaming_app.py [file|kafka|windowed|production|test]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "file":
        query = file_stream_processing()
        query.awaitTermination()
    
    elif mode == "kafka":
        df = kafka_stream_processing()
        query = df.writeStream.format("console").start()
        query.awaitTermination()
    
    elif mode == "windowed":
        tumbling, sliding, sessions = windowed_aggregations()
        queries = [
            tumbling.writeStream.format("console").queryName("tumbling").start(),
            sliding.writeStream.format("console").queryName("sliding").start(),
            sessions.writeStream.format("console").queryName("sessions").start()
        ]
        for q in queries:
            q.awaitTermination()
    
    elif mode == "production":
        production_streaming_app()
    
    elif mode == "test":
        generate_test_data()
    
    elif mode == "simple":
        run_simple_example()
    
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)