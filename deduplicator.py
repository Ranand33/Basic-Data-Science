# ============================================================================
# COMPREHENSIVE DATA DEDUPLICATOR IN PYTHON
# ============================================================================

import hashlib
import json
import os
import pickle
import re
import sqlite3
import time
from collections import defaultdict, Counter
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Set, Tuple, Any, Optional, Callable, Iterator
import pandas as pd
import numpy as np

# ============================================================================
# 1. BASIC EXACT DEDUPLICATION
# ============================================================================

class BasicDeduplicator:
    """Simple exact match deduplication"""
    
    def __init__(self):
        self.seen_hashes = set()
        self.seen_items = set()
    
    def deduplicate_list(self, items: List[Any]) -> List[Any]:
        """Remove exact duplicates from a list"""
        seen = set()
        result = []
        
        for item in items:
            # Handle unhashable types
            try:
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            except TypeError:
                # For unhashable types, convert to string
                item_str = str(item)
                if item_str not in seen:
                    seen.add(item_str)
                    result.append(item)
        
        return result
    
    def deduplicate_by_hash(self, items: List[Any]) -> List[Any]:
        """Deduplicate using hash values"""
        seen_hashes = set()
        result = []
        
        for item in items:
            # Create hash of the item
            item_hash = hashlib.md5(str(item).encode()).hexdigest()
            
            if item_hash not in seen_hashes:
                seen_hashes.add(item_hash)
                result.append(item)
        
        return result
    
    def deduplicate_by_key(self, items: List[Dict], key_fields: List[str]) -> List[Dict]:
        """Deduplicate dictionaries by specific key fields"""
        seen_keys = set()
        result = []
        
        for item in items:
            # Create composite key from specified fields
            key_values = tuple(item.get(field, '') for field in key_fields)
            
            if key_values not in seen_keys:
                seen_keys.add(key_values)
                result.append(item)
        
        return result

# ============================================================================
# 2. ADVANCED FUZZY DEDUPLICATION
# ============================================================================

class FuzzyDeduplicator:
    """Advanced deduplication with fuzzy matching"""
    
    def __init__(self, similarity_threshold: float = 0.8):
        self.similarity_threshold = similarity_threshold
    
    def levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        if len(s1) < len(s2):
            return self.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def jaccard_similarity(self, s1: str, s2: str) -> float:
        """Calculate Jaccard similarity between two strings"""
        set1 = set(s1.lower().split())
        set2 = set(s2.lower().split())
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def cosine_similarity(self, s1: str, s2: str) -> float:
        """Calculate cosine similarity between two strings"""
        words1 = s1.lower().split()
        words2 = s2.lower().split()
        
        # Create word frequency vectors
        all_words = set(words1 + words2)
        vec1 = [words1.count(word) for word in all_words]
        vec2 = [words2.count(word) for word in all_words]
        
        # Calculate cosine similarity
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = sum(a * a for a in vec1) ** 0.5
        magnitude2 = sum(b * b for b in vec2) ** 0.5
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def phonetic_similarity(self, s1: str, s2: str) -> float:
        """Simple phonetic similarity using Soundex-like algorithm"""
        def soundex_simplified(s):
            s = re.sub(r'[^a-zA-Z]', '', s.upper())
            if not s:
                return "0000"
            
            # Keep first letter
            result = s[0]
            
            # Replace similar sounding letters
            replacements = {
                'BFPV': '1', 'CGJKQSXZ': '2', 'DT': '3', 'L': '4', 'MN': '5', 'R': '6'
            }
            
            for group, digit in replacements.items():
                for char in group:
                    s = s.replace(char, digit)
            
            # Remove duplicates and vowels
            s = re.sub(r'([0-9])\1+', r'\1', s)
            s = re.sub(r'[AEIOUY]', '', s[1:])
            
            result += s[:3].ljust(3, '0')
            return result[:4]
        
        soundex1 = soundex_simplified(s1)
        soundex2 = soundex_simplified(s2)
        
        return 1.0 if soundex1 == soundex2 else 0.0
    
    def combined_similarity(self, s1: str, s2: str) -> float:
        """Combine multiple similarity metrics"""
        # Normalize strings
        s1_clean = re.sub(r'[^\w\s]', '', s1.lower().strip())
        s2_clean = re.sub(r'[^\w\s]', '', s2.lower().strip())
        
        if not s1_clean or not s2_clean:
            return 0.0
        
        # Calculate different similarities
        jaccard = self.jaccard_similarity(s1_clean, s2_clean)
        cosine = self.cosine_similarity(s1_clean, s2_clean)
        
        # Levenshtein similarity (normalized)
        max_len = max(len(s1_clean), len(s2_clean))
        levenshtein_sim = 1.0 - (self.levenshtein_distance(s1_clean, s2_clean) / max_len)
        
        # Phonetic similarity
        phonetic = self.phonetic_similarity(s1_clean, s2_clean)
        
        # Weighted combination
        combined = (jaccard * 0.3 + cosine * 0.3 + levenshtein_sim * 0.3 + phonetic * 0.1)
        
        return combined
    
    def deduplicate_fuzzy(self, items: List[str]) -> List[str]:
        """Deduplicate strings using fuzzy matching"""
        if not items:
            return []
        
        result = [items[0]]  # Always include first item
        
        for item in items[1:]:
            is_duplicate = False
            
            for existing_item in result:
                similarity = self.combined_similarity(item, existing_item)
                
                if similarity >= self.similarity_threshold:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                result.append(item)
        
        return result
    
    def find_duplicate_groups(self, items: List[str]) -> List[List[str]]:
        """Group similar items together"""
        groups = []
        remaining_items = items.copy()
        
        while remaining_items:
            current_item = remaining_items.pop(0)
            current_group = [current_item]
            
            # Find similar items
            items_to_remove = []
            for i, item in enumerate(remaining_items):
                similarity = self.combined_similarity(current_item, item)
                if similarity >= self.similarity_threshold:
                    current_group.append(item)
                    items_to_remove.append(i)
            
            # Remove grouped items from remaining
            for i in reversed(items_to_remove):
                remaining_items.pop(i)
            
            groups.append(current_group)
        
        return groups

# ============================================================================
# 3. PROBABILISTIC DEDUPLICATION
# ============================================================================

class ProbabilisticDeduplicator:
    """Probabilistic deduplication using Bloom filters and MinHash"""
    
    def __init__(self, expected_items: int = 10000, false_positive_rate: float = 0.01):
        self.expected_items = expected_items
        self.false_positive_rate = false_positive_rate
        self.bloom_size = self._optimal_bloom_size()
        self.hash_count = self._optimal_hash_count()
        self.bloom_filter = [False] * self.bloom_size
        self.seen_items = set()
    
    def _optimal_bloom_size(self) -> int:
        """Calculate optimal bloom filter size"""
        m = -((self.expected_items * np.log(self.false_positive_rate)) / (np.log(2) ** 2))
        return int(m)
    
    def _optimal_hash_count(self) -> int:
        """Calculate optimal number of hash functions"""
        k = (self.bloom_size / self.expected_items) * np.log(2)
        return max(1, int(k))
    
    def _hash_functions(self, item: str) -> List[int]:
        """Generate multiple hash values for an item"""
        hashes = []
        for i in range(self.hash_count):
            hash_input = f"{item}{i}".encode()
            hash_value = int(hashlib.md5(hash_input).hexdigest(), 16)
            hashes.append(hash_value % self.bloom_size)
        return hashes
    
    def add_to_bloom(self, item: str):
        """Add item to bloom filter"""
        hash_values = self._hash_functions(item)
        for hash_val in hash_values:
            self.bloom_filter[hash_val] = True
    
    def probably_seen(self, item: str) -> bool:
        """Check if item was probably seen before"""
        hash_values = self._hash_functions(item)
        return all(self.bloom_filter[hash_val] for hash_val in hash_values)
    
    def minhash_signature(self, text: str, num_hashes: int = 100) -> List[int]:
        """Generate MinHash signature for text"""
        words = set(text.lower().split())
        signatures = []
        
        for i in range(num_hashes):
            min_hash = float('inf')
            for word in words:
                hash_input = f"{word}{i}".encode()
                hash_val = int(hashlib.md5(hash_input).hexdigest(), 16)
                min_hash = min(min_hash, hash_val)
            signatures.append(min_hash)
        
        return signatures
    
    def minhash_similarity(self, sig1: List[int], sig2: List[int]) -> float:
        """Calculate similarity between MinHash signatures"""
        if len(sig1) != len(sig2):
            return 0.0
        
        matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
        return matches / len(sig1)
    
    def deduplicate_probabilistic(self, items: List[str]) -> List[str]:
        """Deduplicate using probabilistic methods"""
        result = []
        
        for item in items:
            if not self.probably_seen(item):
                result.append(item)
                self.add_to_bloom(item)
                self.seen_items.add(item)
            else:
                # Double-check with exact match to avoid false positives
                if item not in self.seen_items:
                    result.append(item)
                    self.seen_items.add(item)
        
        return result

# ============================================================================
# 4. LARGE-SCALE DEDUPLICATION
# ============================================================================

class ScalableDeduplicator:
    """Efficient deduplication for large datasets"""
    
    def __init__(self, chunk_size: int = 10000, temp_dir: str = "/tmp/dedup"):
        self.chunk_size = chunk_size
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        self.hash_to_file = {}
    
    def external_sort_deduplicate(self, input_file: str, output_file: str, 
                                key_func: Callable = None) -> int:
        """External sort-based deduplication for very large files"""
        
        if key_func is None:
            key_func = lambda x: x.strip()
        
        # Phase 1: Split into sorted chunks
        chunk_files = []
        chunk_number = 0
        
        with open(input_file, 'r') as f:
            while True:
                chunk = []
                for _ in range(self.chunk_size):
                    line = f.readline()
                    if not line:
                        break
                    chunk.append(line.strip())
                
                if not chunk:
                    break
                
                # Sort and deduplicate chunk
                chunk = sorted(set(chunk), key=key_func)
                
                # Write sorted chunk
                chunk_file = self.temp_dir / f"chunk_{chunk_number}.txt"
                with open(chunk_file, 'w') as cf:
                    cf.write('\n'.join(chunk) + '\n')
                
                chunk_files.append(chunk_file)
                chunk_number += 1
        
        # Phase 2: Merge sorted chunks
        return self._merge_sorted_chunks(chunk_files, output_file, key_func)
    
    def _merge_sorted_chunks(self, chunk_files: List[Path], output_file: str,
                           key_func: Callable) -> int:
        """Merge sorted chunks while removing duplicates"""
        
        # Open all chunk files
        file_handles = []
        current_lines = []
        
        for chunk_file in chunk_files:
            fh = open(chunk_file, 'r')
            line = fh.readline().strip()
            if line:
                file_handles.append(fh)
                current_lines.append((key_func(line), line, len(file_handles) - 1))
        
        # Merge with deduplication
        unique_count = 0
        last_written = None
        
        with open(output_file, 'w') as out:
            while current_lines:
                # Find minimum line
                current_lines.sort(key=lambda x: x[0])
                min_key, min_line, file_idx = current_lines.pop(0)
                
                # Write if not duplicate
                if min_line != last_written:
                    out.write(min_line + '\n')
                    last_written = min_line
                    unique_count += 1
                
                # Read next line from same file
                next_line = file_handles[file_idx].readline().strip()
                if next_line:
                    current_lines.append((key_func(next_line), next_line, file_idx))
                else:
                    file_handles[file_idx].close()
        
        # Cleanup
        for chunk_file in chunk_files:
            chunk_file.unlink()
        
        return unique_count
    
    def hash_based_sharding(self, items: List[Any], num_shards: int = 10) -> Dict[int, List[Any]]:
        """Shard data by hash for parallel processing"""
        shards = defaultdict(list)
        
        for item in items:
            item_hash = hash(str(item))
            shard_id = item_hash % num_shards
            shards[shard_id].append(item)
        
        return dict(shards)
    
    def parallel_deduplicate(self, items: List[Any], num_processes: int = 4) -> List[Any]:
        """Deduplicate using parallel processing"""
        from multiprocessing import Pool
        
        # Shard data
        shards = self.hash_based_sharding(items, num_processes)
        shard_list = list(shards.values())
        
        # Process shards in parallel
        with Pool(processes=num_processes) as pool:
            dedup_shards = pool.map(self._deduplicate_shard, shard_list)
        
        # Combine results
        result = []
        for shard in dedup_shards:
            result.extend(shard)
        
        return result
    
    def _deduplicate_shard(self, shard: List[Any]) -> List[Any]:
        """Deduplicate a single shard"""
        return list(set(shard))

# ============================================================================
# 5. DATABASE DEDUPLICATION
# ============================================================================

class DatabaseDeduplicator:
    """Deduplication for database records"""
    
    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.fuzzy_dedup = FuzzyDeduplicator()
    
    def create_sample_table(self):
        """Create sample table with potential duplicates"""
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                phone TEXT,
                address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert sample data with duplicates
        sample_data = [
            ('John Smith', 'john@email.com', '123-456-7890', '123 Main St'),
            ('Jon Smith', 'john@email.com', '123-456-7890', '123 Main Street'),
            ('Jane Doe', 'jane@email.com', '987-654-3210', '456 Oak Ave'),
            ('John Smith', 'j.smith@email.com', '123-456-7890', '123 Main St'),
            ('Alice Johnson', 'alice@email.com', '555-123-4567', '789 Pine Rd'),
            ('Jane Doe', 'jane@email.com', '987-654-3210', '456 Oak Avenue'),
        ]
        
        self.conn.executemany('''
            INSERT INTO customers (name, email, phone, address) 
            VALUES (?, ?, ?, ?)
        ''', sample_data)
        self.conn.commit()
    
    def find_exact_duplicates(self, table: str, columns: List[str]) -> List[Dict]:
        """Find exact duplicates in database table"""
        column_list = ', '.join(columns)
        
        query = f'''
            SELECT {column_list}, COUNT(*) as count
            FROM {table}
            GROUP BY {column_list}
            HAVING COUNT(*) > 1
        '''
        
        cursor = self.conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    
    def find_potential_duplicates(self, table: str, similarity_threshold: float = 0.8) -> List[Tuple]:
        """Find potential duplicates using fuzzy matching"""
        # Get all records
        cursor = self.conn.execute(f'SELECT * FROM {table}')
        records = [dict(row) for row in cursor.fetchall()]
        
        potential_duplicates = []
        
        for i, record1 in enumerate(records):
            for j, record2 in enumerate(records[i + 1:], i + 1):
                # Compare names
                name_similarity = self.fuzzy_dedup.combined_similarity(
                    record1['name'], record2['name']
                )
                
                # Compare addresses if available
                address_similarity = 0.0
                if record1.get('address') and record2.get('address'):
                    address_similarity = self.fuzzy_dedup.combined_similarity(
                        record1['address'], record2['address']
                    )
                
                # Combined similarity
                overall_similarity = (name_similarity + address_similarity) / 2
                
                if overall_similarity >= similarity_threshold:
                    potential_duplicates.append((record1, record2, overall_similarity))
        
        return potential_duplicates
    
    def merge_duplicates(self, table: str, primary_id: int, duplicate_ids: List[int]):
        """Merge duplicate records into primary record"""
        # This is a simplified merge - in practice, you'd have more sophisticated merging logic
        
        # Delete duplicates
        placeholders = ','.join(['?'] * len(duplicate_ids))
        self.conn.execute(f'DELETE FROM {table} WHERE id IN ({placeholders})', duplicate_ids)
        self.conn.commit()
        
        print(f"Merged {len(duplicate_ids)} duplicate records into record {primary_id}")
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# ============================================================================
# 6. FILE DEDUPLICATION
# ============================================================================

class FileDeduplicator:
    """Deduplicate files based on content"""
    
    def __init__(self):
        self.hash_to_files = defaultdict(list)
    
    def calculate_file_hash(self, file_path: str, algorithm: str = 'md5', 
                          chunk_size: int = 8192) -> str:
        """Calculate hash of file content"""
        hash_obj = hashlib.new(algorithm)
        
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(chunk_size):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except IOError:
            return None
    
    def find_duplicate_files(self, directory: str, recursive: bool = True) -> Dict[str, List[str]]:
        """Find duplicate files in directory"""
        path_obj = Path(directory)
        
        if recursive:
            file_paths = path_obj.rglob('*')
        else:
            file_paths = path_obj.glob('*')
        
        # Calculate hashes for all files
        for file_path in file_paths:
            if file_path.is_file():
                file_hash = self.calculate_file_hash(str(file_path))
                if file_hash:
                    self.hash_to_files[file_hash].append(str(file_path))
        
        # Return only files with duplicates
        duplicates = {
            hash_val: files for hash_val, files in self.hash_to_files.items()
            if len(files) > 1
        }
        
        return duplicates
    
    def get_file_info(self, file_path: str) -> Dict:
        """Get detailed file information"""
        path_obj = Path(file_path)
        stat = path_obj.stat()
        
        return {
            'path': str(file_path),
            'name': path_obj.name,
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'created': stat.st_ctime
        }
    
    def recommend_deletion(self, duplicate_groups: Dict[str, List[str]]) -> Dict[str, Dict]:
        """Recommend which files to keep/delete"""
        recommendations = {}
        
        for hash_val, files in duplicate_groups.items():
            file_infos = [self.get_file_info(f) for f in files]
            
            # Sort by modification time (keep newest) and path (prefer shorter paths)
            file_infos.sort(key=lambda x: (-x['modified'], len(x['path'])))
            
            keep_file = file_infos[0]
            delete_files = file_infos[1:]
            
            recommendations[hash_val] = {
                'keep': keep_file,
                'delete': delete_files,
                'total_size_saved': sum(f['size'] for f in delete_files)
            }
        
        return recommendations

# ============================================================================
# 7. CSV/DATAFRAME DEDUPLICATION
# ============================================================================

class DataFrameDeduplicator:
    """Specialized deduplication for pandas DataFrames"""
    
    def __init__(self):
        self.fuzzy_dedup = FuzzyDeduplicator()
    
    def deduplicate_exact(self, df: pd.DataFrame, subset: List[str] = None, 
                         keep: str = 'first') -> pd.DataFrame:
        """Exact deduplication of DataFrame"""
        return df.drop_duplicates(subset=subset, keep=keep)
    
    def deduplicate_fuzzy_column(self, df: pd.DataFrame, column: str, 
                               threshold: float = 0.8) -> pd.DataFrame:
        """Fuzzy deduplication on a specific column"""
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        # Get unique values using fuzzy matching
        unique_values = self.fuzzy_dedup.deduplicate_fuzzy(df[column].astype(str).tolist())
        
        # Create mapping from original to deduplicated values
        value_mapping = {}
        for original_value in df[column].unique():
            for unique_value in unique_values:
                similarity = self.fuzzy_dedup.combined_similarity(str(original_value), unique_value)
                if similarity >= threshold:
                    value_mapping[original_value] = unique_value
                    break
            else:
                value_mapping[original_value] = str(original_value)
        
        # Apply mapping
        df_result = df.copy()
        df_result[column] = df_result[column].map(value_mapping)
        
        return df_result.drop_duplicates()
    
    def find_similar_records(self, df: pd.DataFrame, text_columns: List[str], 
                           threshold: float = 0.8) -> pd.DataFrame:
        """Find similar records across multiple text columns"""
        similar_groups = []
        processed_indices = set()
        
        for i, row1 in df.iterrows():
            if i in processed_indices:
                continue
            
            current_group = [i]
            
            for j, row2 in df.iterrows():
                if j <= i or j in processed_indices:
                    continue
                
                # Calculate similarity across text columns
                similarities = []
                for col in text_columns:
                    if pd.notna(row1[col]) and pd.notna(row2[col]):
                        sim = self.fuzzy_dedup.combined_similarity(str(row1[col]), str(row2[col]))
                        similarities.append(sim)
                
                if similarities and np.mean(similarities) >= threshold:
                    current_group.append(j)
                    processed_indices.add(j)
            
            if len(current_group) > 1:
                similar_groups.append(current_group)
            
            processed_indices.add(i)
        
        # Create result DataFrame with group information
        result_data = []
        for group_id, indices in enumerate(similar_groups):
            for idx in indices:
                row_data = df.loc[idx].to_dict()
                row_data['duplicate_group'] = group_id
                row_data['original_index'] = idx
                result_data.append(row_data)
        
        return pd.DataFrame(result_data)
    
    def deduplicate_with_confidence(self, df: pd.DataFrame, text_columns: List[str]) -> pd.DataFrame:
        """Deduplicate with confidence scores"""
        result_df = df.copy()
        result_df['confidence_score'] = 1.0
        result_df['is_likely_duplicate'] = False
        
        processed_indices = set()
        
        for i, row1 in df.iterrows():
            if i in processed_indices:
                continue
            
            for j, row2 in df.iterrows():
                if j <= i or j in processed_indices:
                    continue
                
                # Calculate similarity
                similarities = []
                for col in text_columns:
                    if col in df.columns and pd.notna(row1[col]) and pd.notna(row2[col]):
                        sim = self.fuzzy_dedup.combined_similarity(str(row1[col]), str(row2[col]))
                        similarities.append(sim)
                
                if similarities:
                    avg_similarity = np.mean(similarities)
                    if avg_similarity > 0.8:
                        # Mark the later record as likely duplicate
                        result_df.loc[j, 'confidence_score'] = 1 - avg_similarity
                        result_df.loc[j, 'is_likely_duplicate'] = True
                        processed_indices.add(j)
        
        return result_df

# ============================================================================
# 8. COMPREHENSIVE DEDUPLICATION PIPELINE
# ============================================================================

@dataclass
class DeduplicationReport:
    """Report of deduplication results"""
    original_count: int
    deduplicated_count: int
    duplicates_removed: int
    duplicate_rate: float
    processing_time: float
    method_used: str
    confidence_scores: List[float] = None

class ComprehensiveDeduplicator:
    """Main deduplication pipeline combining all methods"""
    
    def __init__(self):
        self.basic_dedup = BasicDeduplicator()
        self.fuzzy_dedup = FuzzyDeduplicator()
        self.prob_dedup = ProbabilisticDeduplicator()
        self.scalable_dedup = ScalableDeduplicator()
        self.df_dedup = DataFrameDeduplicator()
    
    def auto_deduplicate(self, data: Any, method: str = 'auto', **kwargs) -> Tuple[Any, DeduplicationReport]:
        """Automatically choose and apply best deduplication method"""
        start_time = time.time()
        original_count = self._get_data_count(data)
        
        if method == 'auto':
            method = self._choose_best_method(data)
        
        # Apply deduplication
        if method == 'exact':
            result = self._apply_exact_deduplication(data, **kwargs)
        elif method == 'fuzzy':
            result = self._apply_fuzzy_deduplication(data, **kwargs)
        elif method == 'probabilistic':
            result = self._apply_probabilistic_deduplication(data, **kwargs)
        elif method == 'scalable':
            result = self._apply_scalable_deduplication(data, **kwargs)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Generate report
        end_time = time.time()
        deduplicated_count = self._get_data_count(result)
        duplicates_removed = original_count - deduplicated_count
        
        report = DeduplicationReport(
            original_count=original_count,
            deduplicated_count=deduplicated_count,
            duplicates_removed=duplicates_removed,
            duplicate_rate=duplicates_removed / original_count if original_count > 0 else 0,
            processing_time=end_time - start_time,
            method_used=method
        )
        
        return result, report
    
    def _get_data_count(self, data: Any) -> int:
        """Get count of items in data"""
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, pd.DataFrame):
            return len(data)
        elif isinstance(data, str):
            return 1
        else:
            return len(str(data).split('\n'))
    
    def _choose_best_method(self, data: Any) -> str:
        """Choose best deduplication method based on data characteristics"""
        data_size = self._get_data_count(data)
        
        if data_size < 1000:
            return 'exact'
        elif data_size < 10000:
            return 'fuzzy'
        elif data_size < 100000:
            return 'probabilistic'
        else:
            return 'scalable'
    
    def _apply_exact_deduplication(self, data: Any, **kwargs) -> Any:
        """Apply exact deduplication"""
        if isinstance(data, list):
            return self.basic_dedup.deduplicate_list(data)
        elif isinstance(data, pd.DataFrame):
            return self.df_dedup.deduplicate_exact(data, **kwargs)
        else:
            # Convert to list and back
            items = str(data).split('\n')
            deduped = self.basic_dedup.deduplicate_list(items)
            return '\n'.join(deduped)
    
    def _apply_fuzzy_deduplication(self, data: Any, **kwargs) -> Any:
        """Apply fuzzy deduplication"""
        threshold = kwargs.get('threshold', 0.8)
        self.fuzzy_dedup.similarity_threshold = threshold
        
        if isinstance(data, list):
            if all(isinstance(item, str) for item in data):
                return self.fuzzy_dedup.deduplicate_fuzzy(data)
            else:
                return self.basic_dedup.deduplicate_list(data)
        elif isinstance(data, pd.DataFrame):
            text_columns = kwargs.get('text_columns', [])
            if text_columns:
                return self.df_dedup.deduplicate_fuzzy_column(data, text_columns[0], threshold)
            else:
                return self.df_dedup.deduplicate_exact(data)
        else:
            items = str(data).split('\n')
            deduped = self.fuzzy_dedup.deduplicate_fuzzy(items)
            return '\n'.join(deduped)
    
    def _apply_probabilistic_deduplication(self, data: Any, **kwargs) -> Any:
        """Apply probabilistic deduplication"""
        if isinstance(data, list):
            items = [str(item) for item in data]
            return self.prob_dedup.deduplicate_probabilistic(items)
        else:
            items = str(data).split('\n')
            deduped = self.prob_dedup.deduplicate_probabilistic(items)
            return '\n'.join(deduped)
    
    def _apply_scalable_deduplication(self, data: Any, **kwargs) -> Any:
        """Apply scalable deduplication"""
        if isinstance(data, list):
            return self.scalable_dedup.parallel_deduplicate(data)
        else:
            items = str(data).split('\n')
            deduped = self.scalable_dedup.parallel_deduplicate(items)
            return '\n'.join(deduped)

# ============================================================================
# 9. EXAMPLE USAGE AND TESTING
# ============================================================================

def generate_test_data():
    """Generate test data with known duplicates"""
    names = [
        "John Smith", "Jon Smith", "J. Smith", "John Smyth",
        "Jane Doe", "Jane Do", "J. Doe",
        "Alice Johnson", "Alice Jonson", "A. Johnson",
        "Bob Wilson", "Robert Wilson", "Bob Willson"
    ]
    
    emails = [
        "john@email.com", "john@email.com", "j.smith@email.com", "john@email.com",
        "jane@email.com", "jane@email.com", "j.doe@email.com",
        "alice@email.com", "alice@email.com", "a.johnson@email.com",
        "bob@email.com", "robert@email.com", "bob@email.com"
    ]
    
    # Create DataFrame
    df = pd.DataFrame({
        'name': names,
        'email': emails,
        'id': range(len(names))
    })
    
    return df, names

def run_comprehensive_demo():
    """Run comprehensive deduplication demo"""
    print("=== Comprehensive Data Deduplication Demo ===\n")
    
    # Generate test data
    test_df, test_names = generate_test_data()
    
    print("Original data:")
    print(test_df)
    print(f"\nOriginal count: {len(test_df)}")
    
    # Initialize comprehensive deduplicator
    deduplicator = ComprehensiveDeduplicator()
    
    # Test different methods
    methods = ['exact', 'fuzzy', 'probabilistic']
    
    for method in methods:
        print(f"\n--- {method.upper()} Deduplication ---")
        
        if method == 'fuzzy':
            result, report = deduplicator.auto_deduplicate(
                test_names, 
                method=method, 
                threshold=0.7
            )
        else:
            result, report = deduplicator.auto_deduplicate(test_names, method=method)
        
        print(f"Method: {report.method_used}")
        print(f"Original count: {report.original_count}")
        print(f"Deduplicated count: {report.deduplicated_count}")
        print(f"Duplicates removed: {report.duplicates_removed}")
        print(f"Duplicate rate: {report.duplicate_rate:.2%}")
        print(f"Processing time: {report.processing_time:.4f} seconds")
        
        if method == 'fuzzy':
            print("Unique names found:")
            for name in result:
                print(f"  - {name}")
    
    # Test DataFrame deduplication
    print(f"\n--- DATAFRAME Deduplication ---")
    df_dedup = DataFrameDeduplicator()
    
    # Exact deduplication
    exact_result = df_dedup.deduplicate_exact(test_df, subset=['email'])
    print(f"Exact deduplication by email: {len(exact_result)} rows")
    
    # Fuzzy deduplication
    fuzzy_result = df_dedup.deduplicate_fuzzy_column(test_df, 'name', threshold=0.7)
    print(f"Fuzzy deduplication by name: {len(fuzzy_result)} rows")
    
    # Find similar records
    similar_records = df_dedup.find_similar_records(test_df, ['name'], threshold=0.7)
    if not similar_records.empty:
        print(f"Similar record groups found: {similar_records['duplicate_group'].nunique()}")
    
    print("\n=== Demo Complete ===")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "demo":
            run_comprehensive_demo()
        elif command == "files":
            # File deduplication example
            file_dedup = FileDeduplicator()
            duplicates = file_dedup.find_duplicate_files(".", recursive=True)
            
            if duplicates:
                print("Duplicate files found:")
                for hash_val, files in duplicates.items():
                    print(f"Hash: {hash_val}")
                    for file_path in files:
                        print(f"  - {file_path}")
            else:
                print("No duplicate files found.")
        
        elif command == "database":
            # Database deduplication example
            db_dedup = DatabaseDeduplicator()
            db_dedup.create_sample_table()
            
            # Find exact duplicates
            exact_dups = db_dedup.find_exact_duplicates('customers', ['email'])
            print("Exact duplicates by email:")
            for dup in exact_dups:
                print(f"  {dup}")
            
            # Find potential duplicates
            potential_dups = db_dedup.find_potential_duplicates('customers')
            print(f"\nPotential duplicates found: {len(potential_dups)}")
            for record1, record2, similarity in potential_dups:
                print(f"  Similarity: {similarity:.2f}")
                print(f"    {record1['name']} vs {record2['name']}")
            
            db_dedup.close()
        
        else:
            print("Available commands: demo, files, database")
    else:
        run_comprehensive_demo()