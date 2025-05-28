// ============================================================================
// PYTHON MAPREDUCE WITH HADOOP STREAMING
// ============================================================================

# mapper.py
#!/usr/bin/env python3
import sys
import re
from datetime import datetime

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            # Parse CSV input: date,product,category,quantity,price
            fields = line.split(',')
            if len(fields) != 5:
                continue
            
            date_str, product, category, quantity, price = fields
            
            # Calculate total sales
            total_sales = float(quantity) * float(price)
            
            # Extract month from date
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            month_key = date_obj.strftime('%Y-%m')
            
            # Emit key-value pairs
            print(f"{category}\t{total_sales}")
            print(f"{month_key}\t{total_sales}")
            print(f"{product}\t{total_sales}")
            
        except (ValueError, IndexError) as e:
            # Skip invalid records
            continue

if __name__ == "__main__":
    main()

# reducer.py
#!/usr/bin/env python3
import sys

def main():
    current_key = None
    current_sum = 0.0
    current_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            key, value = line.split('\t')
            value = float(value)
            
            if current_key == key:
                current_sum += value
                current_count += 1
            else:
                if current_key is not None:
                    # Output results for previous key
                    print(f"{current_key}_total\t{current_sum:.2f}")
                    print(f"{current_key}_count\t{current_count}")
                    print(f"{current_key}_average\t{current_sum/current_count:.2f}")
                
                current_key = key
                current_sum = value
                current_count = 1
                
        except ValueError:
            continue
    
    # Output final key
    if current_key is not None:
        print(f"{current_key}_total\t{current_sum:.2f}")
        print(f"{current_key}_count\t{current_count}")
        print(f"{current_key}_average\t{current_sum/current_count:.2f}")

if __name__ == "__main__":
    main()
