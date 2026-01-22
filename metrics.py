"""
Simple metrics collection without heavy dependencies.
Thread-safe counters and timing utilities.
"""
import time
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict, Optional


@dataclass
class Metrics:
    """
    Lightweight metrics collector.
    
    Tracks:
    - Request counts by endpoint
    - Error counts by type
    - Cache hit/miss rates
    - API latencies (last N samples)
    """
    
    _lock: Lock = field(default_factory=Lock, repr=False)
    _counters: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _latencies: Dict[str, list] = field(default_factory=lambda: defaultdict(list))
    _max_latency_samples: int = 100
    _start_time: float = field(default_factory=time.time)
    
    def inc(self, name: str, value: int = 1) -> None:
        """Increment counter."""
        with self._lock:
            self._counters[name] += value
    
    def record_latency(self, name: str, duration: float) -> None:
        """Record latency sample."""
        with self._lock:
            samples = self._latencies[name]
            samples.append(duration)
            if len(samples) > self._max_latency_samples:
                samples.pop(0)
    
    def get_counter(self, name: str) -> int:
        """Get counter value."""
        with self._lock:
            return self._counters.get(name, 0)
    
    def get_latency_stats(self, name: str) -> Optional[Dict]:
        """Get latency statistics."""
        with self._lock:
            samples = self._latencies.get(name)
            if not samples:
                return None
            
            sorted_samples = sorted(samples)
            n = len(sorted_samples)
            
            return {
                "count": n,
                "min": round(sorted_samples[0] * 1000, 2),
                "max": round(sorted_samples[-1] * 1000, 2),
                "avg": round(sum(sorted_samples) / n * 1000, 2),
                "p50": round(sorted_samples[n // 2] * 1000, 2),
                "p95": round(sorted_samples[int(n * 0.95)] * 1000, 2) if n >= 20 else None,
            }
    
    def snapshot(self) -> Dict:
        """Get all metrics as dict."""
        with self._lock:
            latency_stats = {}
            for name in self._latencies:
                stats = self.get_latency_stats(name)
                if stats:
                    latency_stats[name] = stats
            
            return {
                "uptime_seconds": int(time.time() - self._start_time),
                "counters": dict(self._counters),
                "latencies_ms": latency_stats,
            }
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._counters.clear()
            self._latencies.clear()


# Global metrics instance
metrics = Metrics()


class Timer:
    """Context manager for timing operations."""
    
    __slots__ = ('_name', '_start')
    
    def __init__(self, name: str):
        self._name = name
        self._start: float = 0
    
    def __enter__(self) -> 'Timer':
        self._start = time.perf_counter()
        return self
    
    def __exit__(self, *args) -> None:
        duration = time.perf_counter() - self._start
        metrics.record_latency(self._name, duration)


def timed(name: str):
    """Decorator to time async functions."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                metrics.inc(f"{name}.success")
                return result
            except Exception:
                metrics.inc(f"{name}.error")
                raise
            finally:
                duration = time.perf_counter() - start
                metrics.record_latency(name, duration)
        return wrapper
    return decorator
