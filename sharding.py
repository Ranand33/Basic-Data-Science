# ============================================================================
# DATA SHARDING AND PARALLEL PROCESSING IN PYTHON
# ============================================================================

import multiprocessing as mp
import concurrent.futures
import threading
import queue
import time
import os
import sys
import pickle
import json
import pandas as pd
import numpy as np
from typing import List, Callable, Any, Iterator, Tuple
from functools import partial
from pathlib import Path
import hashlib
import math

# ============================================================================
# 1. BASIC DATA SHARDING STRATEGIES
# ============================================================================

class DataSharder:
    """Flexible data sharding utility"""
    
    def __init__(self, num_shards: int = None):
        self.num_shards = num_shards or mp.cpu_count()
    
    def shard_by_size(self, data: List[Any], chunk_size: int = None) -> List[List[Any]]:
        """Split data into chunks of specified size"""
        if chunk_size is None:
            chunk_size = max(1, len(data) // self.num_shards)
        
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    def shard_by_count(self, data: List[Any]) -> List[List[Any]]:
        """Split data into fixed number of shards"""
        chunk_size = math.ceil(len(data) / self.num_shards)
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    def shard_by_hash(self, data: List[Any], key_func: Callable = None) -> List[List[Any]]:
        """Split data using hash-based partitioning"""
        if key_func is None:
            key_func = lambda x: str(x)
        
        shards = [[] for _ in range(self.num_shards)]
        
        for item in data:
            hash_value = int(hashlib.md5(key_func(item).encode()).hexdigest(), 16)
            shard_idx = hash_value % self.num_shards
            shards[shard_idx].append(item)
        
        return shards
    
    def shard_by_key(self, data: List[dict], key_field: str) -> dict:
        """Split data by unique key values"""
        shards = {}
        for item in data:
            key_value = item.get(key_field)
            if key_value not in shards:
                shards[key_value] = []
            shards[key_value].append(item)
        
        return shards
    
    def shard_range_based(self, data: List[Any], ranges: List[Tuple]) -> List[List[Any]]:
        """Split data based on value ranges"""
        shards = [[] for _ in range(len(ranges))]
        
        for item in data:
            value = float(item) if isinstance(item, (int, float)) else len(str(item))
            
            for i, (min_val, max_val) in enumerate(ranges):
                if min_val <= value < max_val:
                    shards[i].append(item)
                    break
            else:
                # Default to last shard if no range matches
                shards[-1].append(item)
        
        return shards

# ============================================================================
# 2. MULTIPROCESSING WITH PROCESS POOLS
# ============================================================================

class MultiprocessingSharder:
    """Process-based parallel processing"""
    
    def __init__(self, num_processes: int = None):
        self.num_processes = num_processes or mp.cpu_count()
        self.sharder = DataSharder(self.num_processes)
    
    def process_with_pool(self, data: List[Any], process_func: Callable, 
                         chunk_size: int = None) -> List[Any]:
        """Process data using multiprocessing Pool"""
        
        # Shard the data
        shards = self.sharder.shard_by_count(data)
        
        # Process shards in parallel
        with mp.Pool(processes=self.num_processes) as pool:
            results = pool.map(process_func, shards)
        
        # Flatten results
        return [item for sublist in results for item in sublist]
    
    def process_with_starmap(self, data: List[Tuple], process_func: Callable) -> List[Any]:
        """Process data with multiple arguments using starmap"""
        
        with mp.Pool(processes=self.num_processes) as pool:
            results = pool.starmap(process_func, data)
        
        return results
    
    def process_with_async(self, data: List[Any], process_func: Callable) -> List[Any]:
        """Asynchronous processing with callbacks"""
        
        shards = self.sharder.shard_by_count(data)
        results = []
        
        def collect_result(result):
            results.extend(result)
        
        def handle_error(error):
            print(f"Error in processing: {error}")
        
        with mp.Pool(processes=self.num_processes) as pool:
            # Submit all jobs asynchronously
            async_results = []
            for shard in shards:
                async_result = pool.apply_async(
                    process_func, 
                    args=(shard,),
                    callback=collect_result,
                    error_callback=handle_error
                )
                async_results.append(async_result)
            
            # Wait for all to complete
            for async_result in async_results:
                async_result.wait()
        
        return results

# ============================================================================
# 3. CONCURRENT FUTURES FOR FLEXIBLE PARALLELISM
# ============================================================================

class ConcurrentProcessor:
    """Thread and process-based concurrent processing"""
    
    def __init__(self, max_workers: int = None, use_processes: bool = True):
        self.max_workers = max_workers or mp.cpu_count()
        self.use_processes = use_processes
        self.sharder = DataSharder(self.max_workers)
    
    def process_concurrent(self, data: List[Any], process_func: Callable, 
                          timeout: int = None) -> List[Any]:
        """Process data using concurrent.futures"""
        
        shards = self.sharder.shard_by_count(data)
        
        executor_class = (concurrent.futures.ProcessPoolExecutor if self.use_processes 
                         else concurrent.futures.ThreadPoolExecutor)
        
        with executor_class(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_shard = {
                executor.submit(process_func, shard): i 
                for i, shard in enumerate(shards)
            }
            
            results = [None] * len(shards)
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_shard, timeout=timeout):
                shard_idx = future_to_shard[future]
                try:
                    result = future.result()
                    results[shard_idx] = result
                except Exception as exc:
                    print(f'Shard {shard_idx} generated an exception: {exc}')
                    results[shard_idx] = []
        
        # Flatten results
        return [item for sublist in results if sublist for item in sublist]
    
    def process_with_progress(self, data: List[Any], process_func: Callable) -> List[Any]:
        """Process with progress tracking"""
        
        shards = self.sharder.shard_by_count(data)
        executor_class = (concurrent.futures.ProcessPoolExecutor if self.use_processes 
                         else concurrent.futures.ThreadPoolExecutor)
        
        with executor_class(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_func, shard) for shard in shards]
            
            results = []
            completed = 0
            total = len(futures)
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.extend(result)
                    completed += 1
                    print(f"Progress: {completed}/{total} shards completed ({completed/total*100:.1f}%)")
                except Exception as exc:
                    print(f'Shard generated an exception: {exc}')
                    completed += 1
        
        return results

# ============================================================================
# 4. DISTRIBUTED PROCESSING WITH DASK
# ============================================================================

def setup_dask_processing():
    """Setup Dask for distributed processing"""
    
    try:
        import dask
        import dask.bag as db
        import dask.dataframe as dd
        from dask.distributed import Client, as_completed
        from dask import delayed
        
        class DaskProcessor:
            """Distributed processing with Dask"""
            
            def __init__(self, scheduler_address: str = None):
                if scheduler_address:
                    self.client = Client(scheduler_address)
                else:
                    # Local cluster
                    self.client = Client(processes=True, threads_per_worker=2)
                
                print(f"Dask dashboard: {self.client.dashboard_link}")
            
            def process_bag(self, data: List[Any], process_func: Callable, 
                           num_partitions: int = None) -> List[Any]:
                """Process data using Dask Bag"""
                
                if num_partitions is None:
                    num_partitions = len(self.client.scheduler_info()['workers'])
                
                # Create bag and partition data
                bag = db.from_sequence(data, npartitions=num_partitions)
                
                # Apply processing function
                processed_bag = bag.map(process_func)
                
                # Compute results
                return processed_bag.compute()
            
            def process_dataframe(self, df: pd.DataFrame, process_func: Callable,
                                num_partitions: int = None) -> pd.DataFrame:
                """Process DataFrame using Dask DataFrame"""
                
                if num_partitions is None:
                    num_partitions = len(self.client.scheduler_info()['workers'])
                
                # Convert to Dask DataFrame
                ddf = dd.from_pandas(df, npartitions=num_partitions)
                
                # Apply processing function
                processed_ddf = ddf.apply(process_func, axis=1, meta=df.dtypes)
                
                # Compute results
                return processed_ddf.compute()
            
            def process_delayed(self, data: List[Any], process_func: Callable) -> List[Any]:
                """Process using Dask delayed operations"""
                
                # Create delayed objects
                delayed_results = []
                for item in data:
                    delayed_result = delayed(process_func)(item)
                    delayed_results.append(delayed_result)
                
                # Compute all at once
                results = dask.compute(*delayed_results)
                return list(results)
            
            def process_with_scatter(self, large_data: Any, process_func: Callable, 
                                   data_chunks: List[Any]) -> List[Any]:
                """Process with scattered large data"""
                
                # Scatter large data to all workers
                scattered_data = self.client.scatter(large_data, broadcast=True)
                
                # Process chunks with access to scattered data
                futures = []
                for chunk in data_chunks:
                    future = self.client.submit(process_func, chunk, scattered_data)
                    futures.append(future)
                
                # Gather results
                results = self.client.gather(futures)
                return results
            
            def close(self):
                """Close Dask client"""
                self.client.close()
        
        return DaskProcessor
    
    except ImportError:
        print("Dask not installed. Install with: pip install dask[complete]")
        return None

# ============================================================================
# 5. RAY FOR DISTRIBUTED PROCESSING
# ============================================================================

def setup_ray_processing():
    """Setup Ray for distributed processing"""
    
    try:
        import ray
        
        @ray.remote
        class RayProcessor:
            """Distributed processing with Ray"""
            
            def __init__(self):
                pass
            
            def process_chunk(self, data_chunk: List[Any], process_func: Callable) -> List[Any]:
                """Process a chunk of data"""
                return [process_func(item) for item in data_chunk]
        
        @ray.remote
        def ray_process_function(data_chunk: List[Any], process_func: Callable) -> List[Any]:
            """Remote function for processing data chunks"""
            return [process_func(item) for item in data_chunk]
        
        class RayParallelProcessor:
            """Ray-based parallel processing"""
            
            def __init__(self, num_workers: int = None):
                if not ray.is_initialized():
                    ray.init()
                
                self.num_workers = num_workers or ray.cluster_resources().get('CPU', 1)
                self.sharder = DataSharder(int(self.num_workers))
            
            def process_with_actors(self, data: List[Any], process_func: Callable) -> List[Any]:
                """Process using Ray actors"""
                
                # Create actor pool
                actors = [RayProcessor.remote() for _ in range(int(self.num_workers))]
                
                # Shard data
                shards = self.sharder.shard_by_count(data)
                
                # Process shards using actors
                futures = []
                for i, shard in enumerate(shards):
                    actor = actors[i % len(actors)]
                    future = actor.process_chunk.remote(shard, process_func)
                    futures.append(future)
                
                # Get results
                results = ray.get(futures)
                
                # Flatten results
                return [item for sublist in results for item in sublist]
            
            def process_with_tasks(self, data: List[Any], process_func: Callable) -> List[Any]:
                """Process using Ray tasks"""
                
                # Shard data
                shards = self.sharder.shard_by_count(data)
                
                # Submit tasks
                futures = [ray_process_function.remote(shard, process_func) for shard in shards]
                
                # Get results
                results = ray.get(futures)
                
                # Flatten results
                return [item for sublist in results for item in sublist]
            
            def process_with_batching(self, data: List[Any], process_func: Callable, 
                                    batch_size: int = 1000) -> List[Any]:
                """Process large datasets with batching"""
                
                # Create batches
                batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
                
                all_results = []
                
                # Process batches
                for batch in batches:
                    shards = self.sharder.shard_by_count(batch)
                    futures = [ray_process_function.remote(shard, process_func) for shard in shards]
                    batch_results = ray.get(futures)
                    
                    # Flatten batch results
                    all_results.extend([item for sublist in batch_results for item in sublist])
                
                return all_results
            
            def shutdown(self):
                """Shutdown Ray"""
                ray.shutdown()
        
        return RayParallelProcessor
    
    except ImportError:
        print("Ray not installed. Install with: pip install ray")
        return None

# ============================================================================
# 6. FILE-BASED SHARDING FOR LARGE DATASETS
# ============================================================================

class FileBasedSharder:
    """Shard large files for parallel processing"""
    
    def __init__(self, temp_dir: str = "/tmp/shards"):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
    
    def shard_csv_file(self, file_path: str, num_shards: int = None, 
                      chunk_size: int = 10000) -> List[str]:
        """Shard large CSV file into smaller files"""
        
        if num_shards is None:
            num_shards = mp.cpu_count()
        
        shard_files = []
        
        # Read and distribute rows
        df_chunks = pd.read_csv(file_path, chunksize=chunk_size)
        
        current_shard = 0
        rows_in_current_shard = 0
        current_shard_data = []
        
        for chunk in df_chunks:
            for _, row in chunk.iterrows():
                current_shard_data.append(row.to_dict())
                rows_in_current_shard += 1
                
                # Switch to next shard when current is full
                if rows_in_current_shard >= chunk_size:
                    shard_file = self.temp_dir / f"shard_{current_shard}.json"
                    with open(shard_file, 'w') as f:
                        json.dump(current_shard_data, f)
                    
                    shard_files.append(str(shard_file))
                    current_shard = (current_shard + 1) % num_shards
                    current_shard_data = []
                    rows_in_current_shard = 0
        
        # Handle remaining data
        if current_shard_data:
            shard_file = self.temp_dir / f"shard_{current_shard}_final.json"
            with open(shard_file, 'w') as f:
                json.dump(current_shard_data, f)
            shard_files.append(str(shard_file))
        
        return shard_files
    
    def shard_text_file(self, file_path: str, num_shards: int = None) -> List[str]:
        """Shard large text file by lines"""
        
        if num_shards is None:
            num_shards = mp.cpu_count()
        
        # Count total lines
        with open(file_path, 'r') as f:
            total_lines = sum(1 for _ in f)
        
        lines_per_shard = total_lines // num_shards
        shard_files = []
        
        with open(file_path, 'r') as f:
            for shard_idx in range(num_shards):
                shard_file = self.temp_dir / f"text_shard_{shard_idx}.txt"
                
                with open(shard_file, 'w') as shard_f:
                    lines_written = 0
                    for line in f:
                        shard_f.write(line)
                        lines_written += 1
                        
                        if lines_written >= lines_per_shard and shard_idx < num_shards - 1:
                            break
                
                shard_files.append(str(shard_file))
        
        return shard_files
    
    def process_file_shards(self, shard_files: List[str], process_func: Callable) -> List[Any]:
        """Process file shards in parallel"""
        
        def process_file_shard(file_path: str):
            """Process a single file shard"""
            if file_path.endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
            else:
                with open(file_path, 'r') as f:
                    data = f.readlines()
            
            return process_func(data)
        
        # Process shards in parallel
        with mp.Pool() as pool:
            results = pool.map(process_file_shard, shard_files)
        
        return results
    
    def cleanup_shards(self):
        """Clean up temporary shard files"""
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

# ============================================================================
# 7. MEMORY-AWARE PROCESSING
# ============================================================================

class MemoryAwareProcessor:
    """Process data with memory management"""
    
    def __init__(self, max_memory_mb: int = 1024):
        self.max_memory_mb = max_memory_mb
        self.sharder = DataSharder()
    
    def estimate_memory_usage(self, sample_data: List[Any], sample_size: int = 100) -> float:
        """Estimate memory usage per item"""
        if len(sample_data) < sample_size:
            sample = sample_data
        else:
            sample = sample_data[:sample_size]
        
        # Serialize sample to estimate size
        serialized = pickle.dumps(sample)
        bytes_per_item = len(serialized) / len(sample)
        
        return bytes_per_item / (1024 * 1024)  # Convert to MB
    
    def adaptive_sharding(self, data: List[Any]) -> List[List[Any]]:
        """Adaptive sharding based on memory constraints"""
        
        memory_per_item = self.estimate_memory_usage(data)
        items_per_shard = int(self.max_memory_mb / memory_per_item)
        
        # Ensure at least one item per shard
        items_per_shard = max(1, items_per_shard)
        
        return self.sharder.shard_by_size(data, items_per_shard)
    
    def process_with_memory_monitoring(self, data: List[Any], process_func: Callable) -> List[Any]:
        """Process with memory monitoring"""
        
        import psutil
        
        def monitored_process_func(shard):
            """Wrapper function with memory monitoring"""
            process = psutil.Process()
            initial_memory = process.memory_info().rss / (1024 * 1024)  # MB
            
            result = process_func(shard)
            
            final_memory = process.memory_info().rss / (1024 * 1024)  # MB
            memory_used = final_memory - initial_memory
            
            print(f"Process {os.getpid()}: Memory used: {memory_used:.2f} MB")
            
            return result
        
        # Adaptive sharding
        shards = self.adaptive_sharding(data)
        
        # Process with monitoring
        with mp.Pool() as pool:
            results = pool.map(monitored_process_func, shards)
        
        return [item for sublist in results for item in sublist]

# ============================================================================
# 8. FAULT-TOLERANT PROCESSING
# ============================================================================

class FaultTolerantProcessor:
    """Process data with fault tolerance and retry logic"""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.sharder = DataSharder()
    
    def process_with_retry(self, data: List[Any], process_func: Callable) -> Tuple[List[Any], List[Any]]:
        """Process data with retry logic"""
        
        def resilient_process_func(shard_info):
            """Process function with retry logic"""
            shard_idx, shard_data = shard_info
            
            for attempt in range(self.max_retries + 1):
                try:
                    result = process_func(shard_data)
                    return (shard_idx, 'success', result)
                except Exception as e:
                    if attempt < self.max_retries:
                        print(f"Shard {shard_idx}, attempt {attempt + 1} failed: {e}. Retrying...")
                        time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        print(f"Shard {shard_idx} failed after {self.max_retries + 1} attempts: {e}")
                        return (shard_idx, 'failed', str(e))
        
        # Shard data with indices
        shards = self.sharder.shard_by_count(data)
        shard_info = [(i, shard) for i, shard in enumerate(shards)]
        
        # Process with retry logic
        with mp.Pool() as pool:
            results = pool.map(resilient_process_func, shard_info)
        
        # Separate successful and failed results
        successful_results = []
        failed_shards = []
        
        for shard_idx, status, result in results:
            if status == 'success':
                successful_results.extend(result)
            else:
                failed_shards.append((shard_idx, result))
        
        return successful_results, failed_shards
    
    def process_with_checkpointing(self, data: List[Any], process_func: Callable, 
                                 checkpoint_dir: str = "/tmp/checkpoints") -> List[Any]:
        """Process data with checkpointing for recovery"""
        
        checkpoint_path = Path(checkpoint_dir)
        checkpoint_path.mkdir(exist_ok=True)
        
        shards = self.sharder.shard_by_count(data)
        results = []
        
        for i, shard in enumerate(shards):
            checkpoint_file = checkpoint_path / f"shard_{i}.pkl"
            
            # Check if already processed
            if checkpoint_file.exists():
                print(f"Loading cached result for shard {i}")
                with open(checkpoint_file, 'rb') as f:
                    shard_result = pickle.load(f)
            else:
                try:
                    print(f"Processing shard {i}")
                    shard_result = process_func(shard)
                    
                    # Save checkpoint
                    with open(checkpoint_file, 'wb') as f:
                        pickle.dump(shard_result, f)
                except Exception as e:
                    print(f"Error processing shard {i}: {e}")
                    continue
            
            results.extend(shard_result)
        
        return results

# ============================================================================
# 9. PERFORMANCE MONITORING AND OPTIMIZATION
# ============================================================================

class PerformanceMonitor:
    """Monitor and optimize parallel processing performance"""
    
    def __init__(self):
        self.metrics = {}
    
    def benchmark_sharding_strategies(self, data: List[Any], process_func: Callable) -> dict:
        """Benchmark different sharding strategies"""
        
        strategies = {
            'by_count': lambda d: DataSharder().shard_by_count(d),
            'by_size_small': lambda d: DataSharder().shard_by_size(d, len(d) // (mp.cpu_count() * 2)),
            'by_size_large': lambda d: DataSharder().shard_by_size(d, len(d) // (mp.cpu_count() // 2)),
            'by_hash': lambda d: DataSharder().shard_by_hash(d)
        }
        
        results = {}
        
        for strategy_name, strategy_func in strategies.items():
            start_time = time.time()
            
            try:
                shards = strategy_func(data)
                
                with mp.Pool() as pool:
                    processed_results = pool.map(process_func, shards)
                
                end_time = time.time()
                
                results[strategy_name] = {
                    'execution_time': end_time - start_time,
                    'num_shards': len(shards),
                    'avg_shard_size': sum(len(shard) for shard in shards) / len(shards),
                    'results_count': sum(len(result) for result in processed_results)
                }
            except Exception as e:
                results[strategy_name] = {'error': str(e)}
        
        return results
    
    def profile_processing(self, data: List[Any], process_func: Callable) -> dict:
        """Profile processing performance"""
        
        import cProfile
        import pstats
        from io import StringIO
        
        # Profile sequential processing
        pr_sequential = cProfile.Profile()
        pr_sequential.enable()
        
        sequential_start = time.time()
        sequential_results = [process_func([item]) for item in data]
        sequential_time = time.time() - sequential_start
        
        pr_sequential.disable()
        
        # Profile parallel processing
        parallel_start = time.time()
        parallel_processor = MultiprocessingSharder()
        parallel_results = parallel_processor.process_with_pool(data, process_func)
        parallel_time = time.time() - parallel_start
        
        # Generate profile stats
        s = StringIO()
        ps = pstats.Stats(pr_sequential, stream=s)
        ps.sort_stats('cumulative')
        ps.print_stats(10)
        
        profile_info = {
            'sequential_time': sequential_time,
            'parallel_time': parallel_time,
            'speedup': sequential_time / parallel_time,
            'efficiency': (sequential_time / parallel_time) / mp.cpu_count(),
            'profile_stats': s.getvalue()
        }
        
        return profile_info

# ============================================================================
# 10. EXAMPLE USAGE AND DEMONSTRATIONS
# ============================================================================

def example_data_processing_tasks():
    """Example processing tasks for demonstration"""
    
    def cpu_intensive_task(data_chunk: List[int]) -> List[int]:
        """Simulate CPU-intensive processing"""
        results = []
        for item in data_chunk:
            # Simulate complex computation
            result = sum(i * i for i in range(item % 1000))
            results.append(result)
        return results
    
    def io_intensive_task(data_chunk: List[str]) -> List[int]:
        """Simulate I/O-intensive processing"""
        results = []
        for item in data_chunk:
            # Simulate file operations
            time.sleep(0.01)  # Simulate I/O delay
            results.append(len(item))
        return results
    
    def memory_intensive_task(data_chunk: List[Any]) -> List[Any]:
        """Simulate memory-intensive processing"""
        # Create large temporary structures
        temp_data = {}
        results = []
        
        for item in data_chunk:
            # Simulate memory-intensive operations
            temp_data[str(item)] = [item] * 100
            results.append(sum(temp_data[str(item)]))
        
        return results
    
    return cpu_intensive_task, io_intensive_task, memory_intensive_task

def run_comprehensive_example():
    """Run comprehensive example demonstrating all techniques"""
    
    print("=== Data Sharding and Parallel Processing Demo ===\n")
    
    # Generate test data
    test_data = list(range(10000))
    cpu_task, io_task, memory_task = example_data_processing_tasks()
    
    # 1. Basic multiprocessing
    print("1. Basic Multiprocessing:")
    mp_processor = MultiprocessingSharder(num_processes=4)
    start_time = time.time()
    mp_results = mp_processor.process_with_pool(test_data, cpu_task)
    mp_time = time.time() - start_time
    print(f"   Processed {len(mp_results)} items in {mp_time:.2f} seconds\n")
    
    # 2. Concurrent futures
    print("2. Concurrent Futures:")
    cf_processor = ConcurrentProcessor(max_workers=4, use_processes=True)
    start_time = time.time()
    cf_results = cf_processor.process_concurrent(test_data, cpu_task)
    cf_time = time.time() - start_time
    print(f"   Processed {len(cf_results)} items in {cf_time:.2f} seconds\n")
    
    # 3. File-based sharding
    print("3. File-based Sharding:")
    file_sharder = FileBasedSharder()
    
    # Create test CSV file
    import pandas as pd
    df = pd.DataFrame({'values': test_data, 'doubled': [x*2 for x in test_data]})
    test_csv = "/tmp/test_data.csv"
    df.to_csv(test_csv, index=False)
    
    shard_files = file_sharder.shard_csv_file(test_csv, num_shards=4)
    print(f"   Created {len(shard_files)} shard files")
    
    def process_csv_shard(data):
        return [item['values'] * 2 for item in data]
    
    file_results = file_sharder.process_file_shards(shard_files, process_csv_shard)
    file_sharder.cleanup_shards()
    print(f"   Processed {sum(len(r) for r in file_results)} items from files\n")
    
    # 4. Fault-tolerant processing
    print("4. Fault-tolerant Processing:")
    
    def unreliable_task(data_chunk):
        """Task that randomly fails"""
        import random
        if random.random() < 0.2:  # 20% failure rate
            raise Exception("Random failure")
        return [x * 3 for x in data_chunk]
    
    ft_processor = FaultTolerantProcessor(max_retries=2)
    successful, failed = ft_processor.process_with_retry(test_data[:1000], unreliable_task)
    print(f"   Successfully processed: {len(successful)} items")
    print(f"   Failed shards: {len(failed)}\n")
    
    # 5. Performance benchmarking
    print("5. Performance Benchmarking:")
    monitor = PerformanceMonitor()
    benchmark_results = monitor.benchmark_sharding_strategies(test_data[:1000], cpu_task)
    
    for strategy, metrics in benchmark_results.items():
        if 'error' not in metrics:
            print(f"   {strategy}: {metrics['execution_time']:.2f}s, "
                  f"{metrics['num_shards']} shards")
    
    print("\n=== Demo Complete ===")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    
    # Set multiprocessing start method for compatibility
    if hasattr(mp, 'set_start_method'):
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass  # Already set
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        
        if mode == "demo":
            run_comprehensive_example()
        elif mode == "dask":
            DaskProcessor = setup_dask_processing()
            if DaskProcessor:
                print("Dask processor available")
        elif mode == "ray":
            RayProcessor = setup_ray_processing()
            if RayProcessor:
                print("Ray processor available")
        else:
            print("Unknown mode. Available modes: demo, dask, ray")
    else:
        run_comprehensive_example()