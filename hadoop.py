# Hadoop Batch Processing with Python
# Multiple approaches for different use cases

# ============================================================================
# 1. HDFS Operations using hdfs3 library
# ============================================================================

from hdfs3 import HDFileSystem
import pandas as pd
import json

class HDFSManager:
    def __init__(self, host='localhost', port=9000):
        self.hdfs = HDFileSystem(host=host, port=port)
    
    def upload_file(self, local_path, hdfs_path):
        """Upload file to HDFS"""
        self.hdfs.put(local_path, hdfs_path)
        print(f"Uploaded {local_path} to {hdfs_path}")
    
    def download_file(self, hdfs_path, local_path):
        """Download file from HDFS"""
        self.hdfs.get(hdfs_path, local_path)
        print(f"Downloaded {hdfs_path} to {local_path}")
    
    def list_directory(self, path='/'):
        """List HDFS directory contents"""
        return self.hdfs.ls(path)
    
    def read_text_file(self, hdfs_path):
        """Read text file from HDFS"""
        with self.hdfs.open(hdfs_path, 'r') as f:
            return f.read()

# ============================================================================
# 2. MapReduce Job using MRJob
# ============================================================================

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class WordCountJob(MRJob):
    """
    Simple word count MapReduce job
    Run with: python wordcount.py input.txt -r hadoop
    """
    
    def mapper(self, _, line):
        # Remove punctuation and convert to lowercase
        words = re.findall(r'\b\w+\b', line.lower())
        for word in words:
            yield word, 1
    
    def reducer(self, word, counts):
        yield word, sum(counts)

class LogAnalysisJob(MRJob):
    """
    More complex log analysis job
    """
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ips,
                   reducer=self.reducer_count_ips),
            MRStep(mapper=self.mapper_sort_by_count,
                   reducer=self.reducer_top_ips)
        ]
    
    def mapper_get_ips(self, _, line):
        # Extract IP addresses from log lines
        # Assuming Apache log format
        parts = line.split()
        if len(parts) > 0:
            ip = parts[0]
            yield ip, 1
    
    def reducer_count_ips(self, ip, counts):
        yield None, (sum(counts), ip)
    
    def mapper_sort_by_count(self, _, count_ip_pair):
        count, ip = count_ip_pair
        yield None, (count, ip)
    
    def reducer_top_ips(self, _, count_ip_pairs):
        # Get top 10 IPs by count
        sorted_pairs = sorted(count_ip_pairs, reverse=True)
        for i, (count, ip) in enumerate(sorted_pairs[:10]):
            yield ip, count

# ============================================================================
# 3. Pydoop for Advanced Hadoop Operations
# ============================================================================

import pydoop.hdfs as hdfs
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

class DataProcessor:
    """Advanced data processing with Pydoop"""
    
    def __init__(self, hdfs_host='default', hdfs_port=0):
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
    
    def process_large_dataset(self, input_path, output_path):
        """Process large dataset in HDFS"""
        
        # Read data in chunks
        with hdfs.open(input_path) as input_file:
            chunk_size = 1024 * 1024  # 1MB chunks
            
            while True:
                chunk = input_file.read(chunk_size)
                if not chunk:
                    break
                
                # Process chunk
                processed_data = self.process_chunk(chunk)
                
                # Write to output
                with hdfs.open(output_path, 'a') as output_file:
                    output_file.write(processed_data)
    
    def process_chunk(self, chunk):
        """Process individual chunk of data"""
        # Your processing logic here
        return chunk.upper()  # Example transformation

# Custom MapReduce with Pydoop
class SalesAnalysisMapper(api.Mapper):
    """Custom mapper for sales data analysis"""
    
    def map(self, context):
        # Process each line of sales data
        line = context.value
        fields = line.strip().split(',')
        
        if len(fields) >= 4:
            date, product, quantity, price = fields[:4]
            try:
                total = float(quantity) * float(price)
                context.emit(product, total)
            except ValueError:
                pass  # Skip invalid records

class SalesAnalysisReducer(api.Reducer):
    """Custom reducer for sales data analysis"""
    
    def reduce(self, context):
        product = context.key
        total_sales = sum(context.values)
        context.emit(product, total_sales)

# ============================================================================
# 4. Batch Processing Pipeline
# ============================================================================

import os
import subprocess
from datetime import datetime, timedelta

class HadoopBatchPipeline:
    """Complete batch processing pipeline"""
    
    def __init__(self, hdfs_manager):
        self.hdfs = hdfs_manager
        self.processing_date = datetime.now().strftime('%Y-%m-%d')
    
    def daily_batch_process(self, input_dir, output_dir):
        """Run daily batch processing"""
        
        print(f"Starting batch process for {self.processing_date}")
        
        # 1. Data ingestion
        self.ingest_data(input_dir)
        
        # 2. Data validation
        valid_files = self.validate_data(input_dir)
        
        # 3. Data processing
        for file_path in valid_files:
            self.process_file(file_path, output_dir)
        
        # 4. Data aggregation
        self.aggregate_results(output_dir)
        
        # 5. Cleanup
        self.cleanup_temp_files()
        
        print("Batch processing completed successfully")
    
    def ingest_data(self, input_dir):
        """Ingest data from various sources"""
        
        # Example: Copy files from local staging to HDFS
        staging_dir = "/local/staging"
        hdfs_input = f"{input_dir}/{self.processing_date}"
        
        if os.path.exists(staging_dir):
            for filename in os.listdir(staging_dir):
                local_file = os.path.join(staging_dir, filename)
                hdfs_file = f"{hdfs_input}/{filename}"
                self.hdfs.upload_file(local_file, hdfs_file)
    
    def validate_data(self, input_dir):
        """Validate input data files"""
        valid_files = []
        hdfs_input = f"{input_dir}/{self.processing_date}"
        
        try:
            files = self.hdfs.list_directory(hdfs_input)
            for file_path in files:
                if self.is_valid_file(file_path):
                    valid_files.append(file_path)
                else:
                    print(f"Invalid file skipped: {file_path}")
        except Exception as e:
            print(f"Error validating files: {e}")
        
        return valid_files
    
    def is_valid_file(self, file_path):
        """Check if file is valid for processing"""
        try:
            # Basic validation - check file size and format
            file_info = self.hdfs.hdfs.info(file_path)
            if file_info['size'] == 0:
                return False
            
            # Additional validation logic here
            return True
        except:
            return False
    
    def process_file(self, input_file, output_dir):
        """Process individual file using MapReduce"""
        
        output_file = f"{output_dir}/{self.processing_date}/{os.path.basename(input_file)}"
        
        # Run MapReduce job
        job_cmd = [
            'python', 'data_processor.py',
            input_file,
            '-r', 'hadoop',
            '--output-dir', output_file
        ]
        
        try:
            result = subprocess.run(job_cmd, check=True, capture_output=True, text=True)
            print(f"Processed {input_file} -> {output_file}")
        except subprocess.CalledProcessError as e:
            print(f"Error processing {input_file}: {e}")
    
    def aggregate_results(self, output_dir):
        """Aggregate processing results"""
        
        results_dir = f"{output_dir}/{self.processing_date}"
        final_output = f"{output_dir}/daily_summary_{self.processing_date}"
        
        # Use MapReduce to aggregate results
        aggregation_job = [
            'python', 'aggregator.py',
            results_dir,
            '-r', 'hadoop',
            '--output-dir', final_output
        ]
        
        try:
            subprocess.run(aggregation_job, check=True)
            print(f"Results aggregated in {final_output}")
        except subprocess.CalledProcessError as e:
            print(f"Error in aggregation: {e}")
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        temp_dir = f"/tmp/hadoop_batch_{self.processing_date}"
        try:
            self.hdfs.hdfs.rm(temp_dir, recursive=True)
            print("Temporary files cleaned up")
        except:
            pass  # Temp dir might not exist

# ============================================================================
# 5. Configuration and Utilities
# ============================================================================

class HadoopConfig:
    """Hadoop configuration management"""
    
    @staticmethod
    def setup_environment():
        """Set up Hadoop environment variables"""
        os.environ['HADOOP_HOME'] = '/path/to/hadoop'
        os.environ['HADOOP_CONF_DIR'] = '/path/to/hadoop/etc/hadoop'
        os.environ['JAVA_HOME'] = '/path/to/java'
        
        # Add Hadoop binaries to PATH
        hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
        if hadoop_bin not in os.environ['PATH']:
            os.environ['PATH'] = f"{hadoop_bin}:{os.environ['PATH']}"
    
    @staticmethod
    def get_hadoop_version():
        """Get Hadoop version"""
        try:
            result = subprocess.run(['hadoop', 'version'], 
                                  capture_output=True, text=True)
            return result.stdout.split('\n')[0]
        except:
            return "Hadoop not found"

# ============================================================================
# 6. Example Usage
# ============================================================================

def main():
    """Example usage of the batch processing pipeline"""
    
    # Setup
    HadoopConfig.setup_environment()
    
    # Initialize HDFS manager
    hdfs_manager = HDFSManager()
    
    # Create batch pipeline
    pipeline = HadoopBatchPipeline(hdfs_manager)
    
    # Run daily batch process
    input_directory = "/data/input"
    output_directory = "/data/output"
    
    pipeline.daily_batch_process(input_directory, output_directory)

if __name__ == "__main__":
    main()

# ============================================================================
# 7. Requirements and Installation
# ============================================================================

"""
Required packages:
pip install mrjob
pip install pydoop
pip install hdfs3
pip install pandas

Hadoop setup requirements:
1. Install Hadoop cluster or use services like EMR, HDInsight
2. Configure HADOOP_HOME and HADOOP_CONF_DIR
3. Ensure Python and required libraries are available on all nodes
4. Configure Hadoop streaming for Python jobs

Example commands to run jobs:
1. Word count: python wordcount.py input.txt -r hadoop
2. Log analysis: python log_analysis.py access.log -r hadoop
3. Custom job: python custom_job.py input/ -r hadoop --output-dir output/
"""