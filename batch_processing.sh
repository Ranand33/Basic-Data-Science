
# ============================================================================
# SHELL SCRIPTS FOR BATCH PROCESSING AUTOMATION
# ============================================================================

#!/bin/bash
# batch_process.sh - Complete batch processing pipeline

set -e  # Exit on any error

# Configuration
HADOOP_HOME="/opt/hadoop"
INPUT_DIR="/data/input"
OUTPUT_DIR="/data/output"
STAGING_DIR="/tmp/staging"
LOG_DIR="/var/log/batch_processing"
DATE=$(date +%Y-%m-%d)

# Functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/batch_$DATE.log"
}

check_hadoop_health() {
    log "Checking Hadoop cluster health..."
    
    if ! hadoop fs -test -d /tmp; then
        log "ERROR: HDFS not accessible"
        exit 1
    fi
    
    # Check if YARN is running
    if ! yarn node -list | grep -q "RUNNING"; then
        log "WARNING: No YARN nodes in RUNNING state"
    fi
    
    log "Hadoop cluster health check passed"
}

ingest_data() {
    log "Starting data ingestion..."
    
    # Create directories if they don't exist
    hadoop fs -mkdir -p "$INPUT_DIR/$DATE"
    hadoop fs -mkdir -p "$OUTPUT_DIR/$DATE"
    
    # Copy new files from staging to HDFS
    if [ -d "$STAGING_DIR" ]; then
        for file in "$STAGING_DIR"/*; do
            if [ -f "$file" ]; then
                log "Uploading $file to HDFS"
                hadoop fs -put "$file" "$INPUT_DIR/$DATE/"
            fi
        done
    fi
    
    log "Data ingestion completed"
}

run_mapreduce_jobs() {
    log "Starting MapReduce jobs..."
    
    # Job 1: Sales Analysis
    log "Running sales analysis job..."
    hadoop jar sales-analysis.jar SalesAnalysis \
        "$INPUT_DIR/$DATE" \
        "$OUTPUT_DIR/$DATE/sales_analysis"
    
    # Job 2: Customer Analysis  
    log "Running customer analysis job..."
    hadoop jar customer-analysis.jar CustomerAnalysis \
        "$INPUT_DIR/$DATE" \
        "$OUTPUT_DIR/$DATE/customer_analysis"
    
    # Job 3: Python streaming job
    log "Running Python streaming job..."
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -files mapper.py,reducer.py \
        -mapper mapper.py \
        -reducer reducer.py \
        -input "$INPUT_DIR/$DATE" \
        -output "$OUTPUT_DIR/$DATE/streaming_analysis"
    
    log "MapReduce jobs completed"
}

run_spark_jobs() {
    log "Starting Spark jobs..."
    
    spark-submit \
        --class SalesAnalysisSpark \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 2g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 10 \
        sales-analysis-spark.jar \
        "$INPUT_DIR/$DATE" \
        "$OUTPUT_DIR/$DATE/spark_analysis"
    
    log "Spark jobs completed"
}

aggregate_results() {
    log "Aggregating results..."
    
    # Combine all analysis results
    hadoop fs -mkdir -p "$OUTPUT_DIR/$DATE/final_results"
    
    # Merge results using Hadoop commands
    hadoop fs -cat "$OUTPUT_DIR/$DATE/sales_analysis/part-*" > \
        "$OUTPUT_DIR/$DATE/final_results/sales_summary.txt"
    
    hadoop fs -cat "$OUTPUT_DIR/$DATE/customer_analysis/part-*" > \
        "$OUTPUT_DIR/$DATE/final_results/customer_summary.txt"
    
    log "Results aggregation completed"
}

generate_reports() {
    log "Generating reports..."
    
    # Create HTML report
    python3 generate_report.py \
        --input-dir "$OUTPUT_DIR/$DATE/final_results" \
        --output-file "$OUTPUT_DIR/$DATE/daily_report_$DATE.html"
    
    # Generate executive summary
    python3 executive_summary.py \
        --input-dir "$OUTPUT_DIR/$DATE/final_results" \
        --output-file "$OUTPUT_DIR/$DATE/executive_summary_$DATE.pdf"
    
    log "Report generation completed"
}

cleanup() {
    log "Cleaning up temporary files..."
    
    # Clean up staging directory
    rm -rf "$STAGING_DIR"/*
    
    # Clean up old output directories (keep last 7 days)
    hadoop fs -ls "$OUTPUT_DIR" | grep "^d" | awk '{print $8}' | \
    while read dir; do
        dir_date=$(basename "$dir")
        if [[ "$dir_date" < $(date -d "7 days ago" +%Y-%m-%d) ]]; then
            log "Removing old directory: $dir"
            hadoop fs -rm -r "$dir"
        fi
    done
    
    log "Cleanup completed"
}

send_notifications() {
    log "Sending notifications..."
    
    # Email notification (requires mail command)
    echo "Batch processing for $DATE completed successfully." | \
    mail -s "Batch Processing Complete - $DATE" admin@company.com
    
    # Slack notification (requires curl and webhook URL)
    curl -X POST -H 'Content-type: application/json' \
        --data "{\"text\":\"Batch processing for $DATE completed successfully.\"}" \
        "$SLACK_WEBHOOK_URL"
    
    log "Notifications sent"
}

# Main execution
main() {
    log "Starting batch processing pipeline for $DATE"
    
    mkdir -p "$LOG_DIR"
    
    check_hadoop_health
    ingest_data
    run_mapreduce_jobs
    run_spark_jobs
    aggregate_results
    generate_reports
    cleanup
    send_notifications
    
    log "Batch processing pipeline completed successfully"
}

# Handle errors
trap 'log "ERROR: Batch processing failed at line $LINENO"' ERR

# Run main function
main "$@"

# ============================================================================
# COMPILATION AND EXECUTION COMMANDS
# ============================================================================

# Java MapReduce compilation:
# export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
# hadoop com.sun.tools.javac.Main WordCount.java
# jar cf wordcount.jar WordCount*.class
# hadoop jar wordcount.jar WordCount input output

# Python Streaming execution:
# hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
#   -files mapper.py,reducer.py \
#   -mapper mapper.py \
#   -reducer reducer.py \
#   -input /input/data \
#   -output /output/results

# Scala Spark compilation:
# sbt package
# spark-submit --class SalesAnalysisSpark target/scala-2.12/sales-analysis_2.12-1.0.jar input output

# Run complete batch pipeline:
# chmod +x batch_process.sh
# ./batch_process.sh