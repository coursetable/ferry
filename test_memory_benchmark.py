#!/usr/bin/env python3
"""
Test script to validate memory benchmarking implementation.
This script creates sample DataFrames and operations to ensure memory tracking works correctly.
"""

import pandas as pd
import numpy as np
import time
import logging
from pathlib import Path

from ferry.memory_benchmark import (
    memory_benchmark,
    memory_checkpoint,
    get_benchmark_data,
    clear_benchmark_data,
    save_benchmark_data,
    print_memory_summary,
    analyze_memory_growth,
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@memory_benchmark(include_dataframes=True, force_gc=True)
def create_large_dataframe(size: int) -> pd.DataFrame:
    """Create a large DataFrame to test memory tracking."""
    print(f"Creating DataFrame with {size} rows...")
    
    data = {
        'id': range(size),
        'random_numbers': np.random.randn(size),
        'categories': np.random.choice(['A', 'B', 'C', 'D'], size),
        'text_data': [f"text_entry_{i}" for i in range(size)],
        'json_like': [{'key': i, 'value': f'data_{i}'} for i in range(size)],
    }
    
    df = pd.DataFrame(data)
    
    # Simulate some processing
    time.sleep(0.1)
    
    return df


@memory_benchmark(include_dataframes=True, force_gc=True)
def process_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Process multiple DataFrames to test memory tracking."""
    
    with memory_checkpoint("merge_dataframes", include_dataframes=True, df1=df1, df2=df2):
        # Merge the dataframes
        merged = pd.merge(df1, df2, on='id', how='inner', suffixes=('_left', '_right'))
    
    with memory_checkpoint("group_operations"):
        # Perform some groupby operations
        grouped = merged.groupby('categories_left').agg({
            'random_numbers_left': ['mean', 'std'],
            'random_numbers_right': ['mean', 'std']
        }).reset_index()
    
    with memory_checkpoint("create_large_intermediate"):
        # Create a large intermediate DataFrame
        large_intermediate = pd.concat([merged] * 3, ignore_index=True)
        
        # Process it
        large_intermediate['computed_column'] = (
            large_intermediate['random_numbers_left'] * 
            large_intermediate['random_numbers_right']
        )
        
        # Delete it to simulate cleanup
        del large_intermediate
    
    return {
        'merged': merged,
        'grouped': grouped,
    }


def simulate_memory_intensive_pipeline():
    """Simulate the memory-intensive operations similar to Ferry pipeline."""
    
    print("Starting memory benchmarking test...")
    clear_benchmark_data()
    
    # Simulate loading multiple course files
    with memory_checkpoint("load_multiple_seasons"):
        dataframes = []
        for season in ['202501', '202502', '202503']:
            df = create_large_dataframe(50000)  # 50k rows per season
            df['season'] = season
            dataframes.append(df)
    
    # Simulate concatenating all seasons
    with memory_checkpoint("concat_all_seasons", include_dataframes=True):
        all_data = pd.concat(dataframes, ignore_index=True)
        # Clean up intermediate dataframes
        del dataframes
    
    # Simulate creating a second large dataset (like evaluations)
    eval_data = create_large_dataframe(30000)  # 30k evaluation records
    
    # Simulate processing both datasets
    result = process_dataframes(all_data, eval_data)
    
    # Simulate database operations
    with memory_checkpoint("simulate_db_operations"):
        # Simulate reading from database
        old_data = create_large_dataframe(45000)
        
        # Simulate diff computation
        time.sleep(0.2)  # Simulate time-intensive operation
        
        # Clean up
        del old_data
    
    print("\nMemory benchmarking test completed!")
    return result


def main():
    """Run the memory benchmarking test."""
    
    # Run the simulation
    result = simulate_memory_intensive_pipeline()
    
    # Print summary
    print_memory_summary()
    
    # Analyze memory growth
    growth_analysis = analyze_memory_growth()
    print("\nMemory Growth Analysis:")
    print(f"Total memory growth: {growth_analysis.get('total_growth_mb', 0):.1f} MB")
    print(f"Peak memory: {growth_analysis.get('peak_memory_mb', 0):.1f} MB")
    
    if 'largest_increases' in growth_analysis:
        print("\nLargest memory increases:")
        for increase in growth_analysis['largest_increases'][:3]:
            print(f"  {increase['from_function']} → {increase['to_function']}: "
                  f"+{increase['memory_growth_mb']:.1f} MB")
    
    # Save benchmark data
    output_path = Path("test_memory_benchmark_results.json")
    save_benchmark_data(output_path)
    print(f"\nDetailed benchmark data saved to: {output_path}")
    
    # Get raw data for analysis
    benchmark_data = get_benchmark_data()
    print(f"\nTotal measurements collected: {len(benchmark_data)}")
    
    # Show DataFrame memory info from the results
    for measurement in benchmark_data[-3:]:  # Show last 3 measurements
        if measurement.dataframe_info:
            print(f"\nDataFrame info for {measurement.function_name} ({measurement.operation}):")
            for df_name, df_info in measurement.dataframe_info.items():
                if isinstance(df_info, dict) and 'memory_mb' in df_info:
                    print(f"  {df_name}: {df_info.get('shape', 'unknown')} shape, "
                          f"{df_info.get('memory_mb', 0):.1f} MB")


if __name__ == "__main__":
    main()
