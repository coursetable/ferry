"""
Memory benchmarking utilities for tracking memory usage throughout the Ferry pipeline.
This module provides decorators and context managers to monitor memory consumption
during data processing operations, particularly for the sync postgres db step.
"""

import functools
import gc
import logging
import psutil
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import pandas as pd
import json

# Type variable for decorated functions
F = TypeVar('F', bound=Callable[..., Any])

# Global memory benchmark storage
_benchmark_data: List["MemoryMeasurement"] = []


@dataclass
class MemoryMeasurement:
    """Data class to store memory measurement information."""
    
    timestamp: float
    function_name: str
    operation: str  # 'start', 'end', 'checkpoint'
    memory_rss_mb: float  # Resident Set Size in MB
    memory_vms_mb: float  # Virtual Memory Size in MB
    memory_percent: float  # Memory percentage of total system
    peak_memory_mb: Optional[float] = None  # Peak memory during operation
    duration_seconds: Optional[float] = None  # Duration for 'end' operations
    dataframe_info: Optional[Dict[str, Any]] = None  # DataFrame memory info
    gc_collected: Optional[int] = None  # Objects collected by garbage collector
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert measurement to dictionary for JSON serialization."""
        return {
            'timestamp': self.timestamp,
            'function_name': self.function_name,
            'operation': self.operation,
            'memory_rss_mb': self.memory_rss_mb,
            'memory_vms_mb': self.memory_vms_mb,
            'memory_percent': self.memory_percent,
            'peak_memory_mb': self.peak_memory_mb,
            'duration_seconds': self.duration_seconds,
            'dataframe_info': self.dataframe_info,
            'gc_collected': self.gc_collected,
        }


class MemoryProfiler:
    """Class to handle memory profiling operations."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.peak_memory = 0.0
        
    def get_current_memory(self) -> tuple[float, float, float]:
        """Get current memory usage."""
        memory_info = self.process.memory_info()
        memory_percent = self.process.memory_percent()
        rss_mb = memory_info.rss / 1024 / 1024  # Convert to MB
        vms_mb = memory_info.vms / 1024 / 1024  # Convert to MB
        
        # Track peak memory
        self.peak_memory = max(self.peak_memory, rss_mb)
        
        return rss_mb, vms_mb, memory_percent
    
    def get_dataframe_memory_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get memory information for a pandas DataFrame."""
        try:
            memory_usage = df.memory_usage(deep=True)
            return {
                'shape': df.shape,
                'memory_mb': memory_usage.sum() / 1024 / 1024,
                'memory_per_column_mb': {
                    str(col): mem / 1024 / 1024 
                    for col, mem in memory_usage.items()
                },
                'dtypes': {str(col): str(dtype) for col, dtype in df.dtypes.items()},
            }
        except Exception as e:
            logging.warning(f"Failed to get DataFrame memory info: {e}")
            return {'error': str(e)}
    
    def force_garbage_collection(self) -> int:
        """Force garbage collection and return number of objects collected."""
        collected = gc.collect()
        return collected


# Global profiler instance
_profiler = MemoryProfiler()


def record_memory_measurement(
    function_name: str,
    operation: str,
    dataframe_info: Optional[Dict[str, Any]] = None,
    duration_seconds: Optional[float] = None,
    gc_collected: Optional[int] = None,
) -> None:
    """Record a memory measurement."""
    rss_mb, vms_mb, memory_percent = _profiler.get_current_memory()
    
    measurement = MemoryMeasurement(
        timestamp=time.time(),
        function_name=function_name,
        operation=operation,
        memory_rss_mb=rss_mb,
        memory_vms_mb=vms_mb,
        memory_percent=memory_percent,
        peak_memory_mb=_profiler.peak_memory if operation == 'end' else None,
        duration_seconds=duration_seconds,
        dataframe_info=dataframe_info,
        gc_collected=gc_collected,
    )
    
    _benchmark_data.append(measurement)
    
    # Log the measurement
    logging.info(
        f"[MEMORY] {function_name} ({operation}): "
        f"RSS={rss_mb:.1f}MB, VMS={vms_mb:.1f}MB, "
        f"MEM%={memory_percent:.1f}%"
        + (f", Duration={duration_seconds:.2f}s" if duration_seconds else "")
        + (f", GC={gc_collected}" if gc_collected is not None else "")
    )


def memory_benchmark(
    include_dataframes: bool = True,
    force_gc: bool = True,
    track_peak: bool = True
) -> Callable[[F], F]:
    """
    Decorator to benchmark memory usage of a function.
    
    Args:
        include_dataframes: Whether to analyze DataFrame memory usage in args/return values
        force_gc: Whether to force garbage collection after function execution
        track_peak: Whether to track peak memory usage during execution
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__name__}"
            
            # Record start measurement
            start_dataframes = None
            if include_dataframes:
                start_dataframes = {}
                for i, arg in enumerate(args):
                    if isinstance(arg, pd.DataFrame):
                        start_dataframes[f'arg_{i}'] = _profiler.get_dataframe_memory_info(arg)
                for key, value in kwargs.items():
                    if isinstance(value, pd.DataFrame):
                        start_dataframes[f'kwarg_{key}'] = _profiler.get_dataframe_memory_info(value)
                    elif isinstance(value, dict):
                        # Check for DataFrames in dict (like tables parameter)
                        for sub_key, sub_value in value.items():
                            if isinstance(sub_value, pd.DataFrame):
                                start_dataframes[f'kwarg_{key}.{sub_key}'] = _profiler.get_dataframe_memory_info(sub_value)
            
            record_memory_measurement(func_name, 'start', dataframe_info=start_dataframes)
            
            # Reset peak memory tracking
            if track_peak:
                _profiler.peak_memory = _profiler.get_current_memory()[0]
            
            start_time = time.time()
            
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Record end measurement
                end_time = time.time()
                duration = end_time - start_time
                
                # Collect garbage if requested
                gc_collected = None
                if force_gc:
                    gc_collected = _profiler.force_garbage_collection()
                
                # Analyze result DataFrames
                end_dataframes = None
                if include_dataframes and result is not None:
                    end_dataframes = {}
                    if isinstance(result, pd.DataFrame):
                        end_dataframes['return_value'] = _profiler.get_dataframe_memory_info(result)
                    elif isinstance(result, dict):
                        for key, value in result.items():
                            if isinstance(value, pd.DataFrame):
                                end_dataframes[f'return.{key}'] = _profiler.get_dataframe_memory_info(value)
                    elif isinstance(result, (list, tuple)):
                        for i, item in enumerate(result):
                            if isinstance(item, pd.DataFrame):
                                end_dataframes[f'return[{i}]'] = _profiler.get_dataframe_memory_info(item)
                
                record_memory_measurement(
                    func_name, 
                    'end', 
                    dataframe_info=end_dataframes,
                    duration_seconds=duration,
                    gc_collected=gc_collected
                )
                
                return result
                
            except Exception as e:
                # Record error measurement
                end_time = time.time()
                duration = end_time - start_time
                record_memory_measurement(
                    func_name, 
                    'error', 
                    duration_seconds=duration
                )
                raise
                
        return wrapper
    return decorator


@contextmanager
def memory_checkpoint(operation_name: str, include_dataframes: bool = False, **dataframe_kwargs):
    """
    Context manager to record memory at specific checkpoints.
    
    Args:
        operation_name: Name of the operation being measured
        include_dataframes: Whether to include DataFrame analysis
        **dataframe_kwargs: DataFrames to analyze (key=name, value=DataFrame)
    """
    # Record start
    start_dataframes = None
    if include_dataframes:
        start_dataframes = {}
        for key, value in dataframe_kwargs.items():
            if isinstance(value, pd.DataFrame):
                start_dataframes[key] = _profiler.get_dataframe_memory_info(value)
    
    record_memory_measurement(operation_name, 'checkpoint_start', dataframe_info=start_dataframes)
    
    start_time = time.time()
    
    try:
        yield
        
        # Record end
        end_time = time.time()
        duration = end_time - start_time
        
        # Force garbage collection
        gc_collected = _profiler.force_garbage_collection()
        
        record_memory_measurement(
            operation_name, 
            'checkpoint_end', 
            duration_seconds=duration,
            gc_collected=gc_collected
        )
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        record_memory_measurement(operation_name, 'checkpoint_error', duration_seconds=duration)
        raise


def get_benchmark_data() -> List[MemoryMeasurement]:
    """Get all recorded benchmark data."""
    return _benchmark_data.copy()


def clear_benchmark_data() -> None:
    """Clear all recorded benchmark data."""
    global _benchmark_data
    _benchmark_data.clear()


def save_benchmark_data(output_path: Union[str, Path]) -> None:
    """Save benchmark data to a JSON file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    data = [measurement.to_dict() for measurement in _benchmark_data]
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    logging.info(f"Memory benchmark data saved to {output_path}")


def print_memory_summary() -> None:
    """Print a summary of memory usage."""
    if not _benchmark_data:
        print("No memory benchmark data available.")
        return
    
    print("\n" + "="*80)
    print("MEMORY BENCHMARK SUMMARY")
    print("="*80)
    
    # Group by function
    functions = {}
    for measurement in _benchmark_data:
        if measurement.function_name not in functions:
            functions[measurement.function_name] = []
        functions[measurement.function_name].append(measurement)
    
    for func_name, measurements in functions.items():
        print(f"\n{func_name}:")
        
        start_measurements = [m for m in measurements if m.operation == 'start']
        end_measurements = [m for m in measurements if m.operation == 'end']
        
        if start_measurements and end_measurements:
            start_mem = start_measurements[0].memory_rss_mb
            end_mem = end_measurements[-1].memory_rss_mb
            peak_mem = end_measurements[-1].peak_memory_mb
            duration = end_measurements[-1].duration_seconds
            
            print(f"  Memory: {start_mem:.1f} MB → {end_mem:.1f} MB (Δ{end_mem-start_mem:+.1f} MB)")
            if peak_mem:
                print(f"  Peak: {peak_mem:.1f} MB")
            if duration:
                print(f"  Duration: {duration:.2f} seconds")
        
        # Print DataFrame info if available
        for measurement in measurements:
            if measurement.dataframe_info:
                print(f"  DataFrame info ({measurement.operation}):")
                for df_name, df_info in measurement.dataframe_info.items():
                    if isinstance(df_info, dict) and 'memory_mb' in df_info:
                        shape = df_info.get('shape', 'unknown')
                        memory_mb = df_info.get('memory_mb', 0)
                        print(f"    {df_name}: {shape} shape, {memory_mb:.1f} MB")
    
    # Overall statistics
    if _benchmark_data:
        max_memory = max(m.memory_rss_mb for m in _benchmark_data)
        print(f"\nOverall Peak Memory: {max_memory:.1f} MB")
    
    print("="*80)


def analyze_memory_growth() -> Dict[str, Any]:
    """Analyze memory growth patterns."""
    if len(_benchmark_data) < 2:
        return {"error": "Insufficient data for analysis"}
    
    # Sort by timestamp
    sorted_data = sorted(_benchmark_data, key=lambda x: x.timestamp)
    
    # Calculate growth between consecutive measurements
    growth_data = []
    for i in range(1, len(sorted_data)):
        prev = sorted_data[i-1]
        curr = sorted_data[i]
        
        growth = curr.memory_rss_mb - prev.memory_rss_mb
        time_diff = curr.timestamp - prev.timestamp
        
        growth_data.append({
            'from_function': prev.function_name,
            'to_function': curr.function_name,
            'memory_growth_mb': growth,
            'time_diff_seconds': time_diff,
            'growth_rate_mb_per_sec': growth / time_diff if time_diff > 0 else 0,
        })
    
    # Find largest memory increases
    largest_increases = sorted(
        [g for g in growth_data if g['memory_growth_mb'] > 0],
        key=lambda x: x['memory_growth_mb'],
        reverse=True
    )[:5]
    
    return {
        'total_growth_mb': sorted_data[-1].memory_rss_mb - sorted_data[0].memory_rss_mb,
        'peak_memory_mb': max(m.memory_rss_mb for m in sorted_data),
        'largest_increases': largest_increases,
        'total_measurements': len(sorted_data),
    }
