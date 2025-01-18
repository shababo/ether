from multiprocessing import Process, Event
import zmq
import time
import json
import statistics
from dataclasses import dataclass
from typing import Dict
import psutil
import os
from ether import (
    ether, ether_pub, ether_sub, ether_save
)
import logging
import tempfile
import uuid

@dataclass
class BenchmarkResult:
    """Holds the results of a single benchmark run"""
    messages_per_second: float    # Throughput
    latency_ms: float            # Average message latency
    cpu_percent: float           # CPU usage during test
    memory_mb: float            # Memory usage during test
    message_loss_percent: float  # Percentage of messages that weren't received
    messages_sent: int          # Total messages sent by publishers
    expected_sent: int          # Total messages expected to be sent (num_messages * num_publishers)
    messages_received: int      # Total messages received across all subscribers
    expected_received: int      # Total messages expected to be received (num_messages * num_subscribers)

class BenchmarkPublisher:
    """Publisher that sends messages to subscribers."""
    def __init__(self, message_size: int):
        self.message_size = message_size
        self.message_count = 0
        self.publisher_id = str(uuid.uuid4())
        self.message = {
            "data": "x" * message_size,
            "timestamp": 0,
            "message_id": 0,
            "publisher_id": self.publisher_id,
            "sequence": 0
        }
    
    @ether_pub(topic="BenchmarkSubscriber.receive_message")
    def publish_message(self, timestamp: float) -> Dict:
        """Publish a single message with current timestamp and unique ID"""
        self.message["timestamp"] = timestamp
        self.message["message_id"] = self.message_count
        self.message["sequence"] = self.message_count
        
        self.message_count += 1
        self._logger.debug(f"Publishing message: {self.message}")
        return self.message

class BenchmarkSubscriber:
    """Subscriber that receives messages and tracks statistics."""
    def __init__(self, results_dir: str = None, subscriber_id: int = 0):
        self.subscriber_id = subscriber_id
        self.results_file = None
        self.received_messages = set()
        self.latencies = []
        self.publishers = {}
        self.first_message_time = None
        self.last_message_time = None
        
        # Set results file if directory provided
        if results_dir:
            self.results_file = os.path.join(results_dir, f"subscriber_{subscriber_id}.json")
    
    @ether_sub()
    def receive_message(self, data: str, timestamp: float, message_id: int, 
                       publisher_id: str, sequence: int):
        """Track received message statistics"""
        try:
            now = time.time()
            latency = (now - timestamp) * 1000  # Convert to ms
            self.latencies.append(latency)
            self.received_messages.add((publisher_id, sequence))
            
            if publisher_id not in self.publishers:
                self.publishers[publisher_id] = {
                    "sequences": set(),
                    "gaps": [],
                    "last_sequence": None,
                    "first_time": now
                }
            
            pub_stats = self.publishers[publisher_id]
            if pub_stats["last_sequence"] is not None:
                expected = pub_stats["last_sequence"] + 1
                if sequence > expected:
                    gap = sequence - expected
                    pub_stats["gaps"].append((expected, sequence, gap))
            
            pub_stats["last_sequence"] = sequence
            pub_stats["sequences"].add(sequence)
            
            # Track message timing
            if self.first_message_time is None:
                self.first_message_time = now
            self.last_message_time = now
            
            # Save results after each message
            # self.save_results()
        except Exception as e:
            self._logger.error(f"Error processing message: {e}", exc_info=True)
    
    @ether_save()
    def save_results(self):
        """Save results to file"""
        if not self.results_file:  # Changed from hasattr check to direct attribute check
            self._logger.warning("No results file path set")
            return
            
        results = {
            "latencies": self.latencies,
            "received_messages": list(self.received_messages),
            "publishers": {
                pid: {
                    "sequences": list(stats["sequences"]),
                    "gaps": stats["gaps"],
                    "last_sequence": stats["last_sequence"]
                }
                for pid, stats in self.publishers.items()
            }
        }
        
        self._logger.debug(f"Saving results to {self.results_file}")
        with open(self.results_file, 'w') as f:
            json.dump(results, f)

    # cleanup all services
    def cleanup(self):
        self._logger.info(f"Cleaning up subscriber {self.subscriber_id}")
        ether.cleanup(self.subscriber_id)



def run_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int) -> BenchmarkResult:
    """Run a single benchmark configuration"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create configuration for subscribers
        config = {
            "instances": {
                f"benchmark_subscriber_{i}": {
                    "class_path": "scripts.benchmark.BenchmarkSubscriber",
                    "kwargs": {
                        "results_dir": temp_dir,
                        "subscriber_id": i,
                        "name": f"benchmark_subscriber_{i}",
                        "log_level": logging.INFO
                    },
                } for i in range(num_subscribers)
            }
        }
        
        # Initialize Ether system with configuration
        ether.tap(
            config=config, 
            # quiet=True,
            restart=True
        )
        
        # Create publishers (these we'll manage manually)
        publishers = []
        for i in range(num_publishers):
            publisher = BenchmarkPublisher(message_size=message_size,log_level = logging.INFO)
            publishers.append(publisher)
        
        # Allow time for connections to establish
        time.sleep(1.0)
        
        # Warmup period
        # warmup_messages = 100
        # messages_per_second = 1000
        # sleep_per_message = 1.0 / messages_per_second
        
        # for publisher in publishers:
        #     for _ in range(warmup_messages):
        #         publisher.publish_message(time.time())
        #         time.sleep(sleep_per_message)
        
        # time.sleep(1.0)  # Additional stabilization time
        
        # Reset message counters after warmup
        for publisher in publishers:
            publisher.message_count = 0
        
        # Monitor resources
        process = psutil.Process(os.getpid())
        
        # Start actual benchmark
        start_time = time.time()
        
        messages_per_publisher = num_messages
        total_messages_sent = 0
        message_interval = 0.0001 * (num_subscribers / 2)
        
        for publisher in publishers:
            for _ in range(messages_per_publisher):
                publisher.publish_message(time.time())
                total_messages_sent += 1
                time.sleep(message_interval)
        
        # Give time for last messages to be processed
        time.sleep(0.5)
        
        # Save results and wait for save to complete
        ether.save()
        time.sleep(0.5)  # Wait for save message to be processed
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Allow time for final messages to be processed
        time.sleep(1.0)  # Reduced from 10.0 as we only need a short wait here
        
        # Collect and process results
        all_latencies = []
        subscriber_results = []
        
        for i in range(num_subscribers):
            result_file = os.path.join(temp_dir, f"subscriber_{i}.json")
            try:
                with open(result_file, 'r') as f:
                    results = json.load(f)
                    # results["received_messages"] = [
                    #     msg for msg in results["received_messages"]
                    #     if isinstance(msg[1], int) and msg[1] < messages_per_publisher
                    # ]
                    all_latencies.extend(results["latencies"])
                    subscriber_results.append(results)
            except FileNotFoundError:
                print(f"Warning: Could not find results file for subscriber {i}")
                print(f"Results file: {result_file}")
                continue
        
        # Calculate metrics
        total_messages_sent = num_messages * num_publishers
        expected_per_sub = total_messages_sent
        total_expected = expected_per_sub * num_subscribers
        
        total_received = sum(len(results["received_messages"]) for results in subscriber_results)
        message_loss_percent = 100 * (1 - total_received / total_expected)
        
        # ether.cleanup_all()
        return BenchmarkResult(
            messages_per_second=total_messages_sent / duration if duration > 0 else 0,
            latency_ms=statistics.mean(all_latencies) if all_latencies else 0,
            cpu_percent=process.cpu_percent(),
            memory_mb=process.memory_info().rss / 1024 / 1024,
            message_loss_percent=message_loss_percent,
            messages_sent=total_messages_sent,
            expected_sent=total_messages_sent,
            messages_received=total_received,
            expected_received=total_expected
        )

def main():
    """Run benchmarks with various configurations"""
    # Configure logging
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    
    # Benchmark parameters
    message_sizes = [10, 1000]
    message_counts = [10000]
    subscriber_counts = [2, 4]
    publisher_counts = [2, 4]
    
    print("\nRunning Ether Benchmark...")
    print("Pubs/Subs | Msg Size | Msg Count | Messages/sec | Latency (ms) | Loss % | Sent/Expected | Received/Expected | Memory (MB)")
    print("-" * 120)
    
    for pub_count in publisher_counts:
        for sub_count in subscriber_counts:
            for size in message_sizes:
                for num_messages in message_counts:
                    print(f"Testing {pub_count}p/{sub_count}s with {size} bytes, {num_messages} msgs... ", end='', flush=True)
                    result = run_benchmark(size, num_messages, sub_count, pub_count)
                    print("\r", end='')
                    print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {num_messages:9d} | {result.messages_per_second:11.2f} | "
                          f"{result.latency_ms:11.2f} | {result.message_loss_percent:6.2f} | "
                          f"{result.messages_sent:6d}/{result.expected_sent:<6d} | "
                          f"{result.messages_received:6d}/{result.expected_received:<6d} | "
                          f"{result.memory_mb:10.1f}")
                    time.sleep(0.1)

if __name__ == "__main__":
    main()