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
    EtherMixin, EtherPubSubProxy, ether_pub, ether_sub, get_logger,
)
import logging
import tempfile
import signal
from threading import Thread
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

class BenchmarkPublisher(EtherMixin):
    """Publisher that sends messages to the broker or subscribers.
    
    Each publisher creates messages of a specific size and sends them with
    timestamps and unique IDs for tracking.
    """
    def __init__(self, message_size: int):
        super().__init__(
            name=f"Publisher-{message_size}bytes",
            log_level=logging.INFO
        )
        # Pre-create message template to avoid allocation during benchmark
        self.message_size = message_size
        self.message_count = 0
        self.publisher_id = str(uuid.uuid4())  # Add unique publisher ID
        self.message = {
            "data": "x" * message_size,  # Fixed-size payload
            "timestamp": 0,              # Will be set at send time
            "message_id": 0,             # Will be incremented for each message
            "publisher_id": self.publisher_id,  # Include publisher ID in message
            "sequence": 0,                # Add sequence number
        }
    
    @ether_pub(topic="BenchmarkSubscriber.receive_message")
    def publish_message(self, timestamp: float) -> Dict:
        """Publish a single message with current timestamp and unique ID"""
        self.message["timestamp"] = timestamp
        self.message["message_id"] = self.message_count
        self.message["sequence"] = self.message_count
        
        self.message_count += 1
        return self.message

class BenchmarkSubscriber(EtherMixin):
    """Subscriber that receives messages and tracks statistics."""
    def __init__(self, results_dir: str, subscriber_id: int):
        super().__init__(
            name=f"Subscriber-{subscriber_id}",
            log_level=logging.INFO,
            results_file=os.path.join(results_dir, f"subscriber_{subscriber_id}.json")
        )
        self.subscriber_id = subscriber_id
    
    @ether_sub()
    def receive_message(self, data: str, timestamp: float, message_id: int, 
                       publisher_id: str, sequence: int):
        self.track_message(publisher_id, sequence, timestamp)


# Helper functions to run components in separate processes
def run_subscriber(stop_event: Event, results_dir: str, subscriber_id: int):
    """Create and run a subscriber in its own process"""
    subscriber = BenchmarkSubscriber(results_dir, subscriber_id)
    subscriber.run(stop_event)

def run_proxy(stop_event: Event):
    """Create and run a proxy in its own process"""
    proxy = EtherPubSubProxy()
    proxy.run(stop_event)

def run_proxy_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int) -> BenchmarkResult:
    stop_event = Event()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Start proxy process (no port parameters needed)
        proxy_process = Process(target=run_proxy, args=(stop_event,))
        proxy_process.start()
        
        # Start subscribers
        sub_processes = []
        for i in range(num_subscribers):
            process = Process(target=run_subscriber, args=(stop_event, temp_dir, i))
            process.start()
            sub_processes.append(process)
        
        # Create publishers
        publishers = []
        for _ in range(num_publishers):
            publisher = BenchmarkPublisher(message_size)
            publisher.setup_sockets()
            publishers.append(publisher)
        
        # Warmup period with scaled sleep time
        warmup_messages = 100
        messages_per_second = 1000  # Target rate during warmup
        sleep_per_message = 1.0 / messages_per_second  # Time to sleep between messages
        
        # Send warmup messages at controlled rate
        for publisher in publishers:
            for _ in range(warmup_messages):
                publisher.publish_message(time.time())
                time.sleep(sleep_per_message)  # Control send rate
        
        # Additional stabilization time proportional to warmup messages
        stabilization_time = 0.1 * (warmup_messages / 1000)  # 0.1s per 1000 messages
        time.sleep(stabilization_time)
        
        # Reset message counters after warmup
        for publisher in publishers:
            publisher.message_count = 0
        
        # Monitor resources
        process = psutil.Process(os.getpid())
        
        # Start actual benchmark
        start_time = time.time()
        
        # Send messages with rate limiting
        messages_per_publisher = num_messages
        total_messages_sent = 0
        message_interval = 0.0001 * (num_subscribers / 2)
        
        for publisher in publishers:
            for _ in range(messages_per_publisher):
                publisher.publish_message(time.time())
                total_messages_sent += 1
                time.sleep(message_interval)
        
        end_time = time.time()
        duration = end_time - start_time
        
        time.sleep(1.0)
        
        stop_event.set()
        for p in sub_processes + [proxy_process]:
            p.join(timeout=5)
        
        # Collect results
        all_latencies = []
        subscriber_results = []
        
        for i in range(num_subscribers):
            result_file = os.path.join(temp_dir, f"subscriber_{i}.json")
            with open(result_file, 'r') as f:
                results = json.load(f)
                # Filter out warmup messages based on sequence numbers
                results["received_messages"] = [
                    msg for msg in results["received_messages"]
                    if isinstance(msg[1], int) and msg[1] < messages_per_publisher
                ]
                all_latencies.extend(results["latencies"])
                subscriber_results.append(results)
        
        for publisher in publishers:
            publisher.cleanup()
        
        # Calculate metrics (excluding warmup)
        total_messages_sent = num_messages * num_publishers
        expected_per_sub = total_messages_sent
        total_expected = expected_per_sub * num_subscribers
        
        total_received = sum(len(results["received_messages"]) for results in subscriber_results)
        message_loss_percent = 100 * (1 - total_received / total_expected)
        
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
    """Run benchmarks with various configurations and display results"""
    # Remove all handlers from root logger
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    
    # Configure single handler for root logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    
    # Benchmark parameters
    message_sizes = [1000, 100000]
    message_counts = [1000, 5000, 100000]
    subscriber_counts = [2, 8]
    publisher_counts = [2, 8]
    
    # Run proxy benchmark
    print("\nRunning XPUB/XSUB Proxy Pattern Benchmark...")
    print("Pubs/Subs | Msg Size | Msg Count | Messages/sec | Latency (ms) | Loss % | Sent/Expected | Received/Expected | Memory (MB)")
    print("-" * 120)
    
    for pub_count in publisher_counts:
        for sub_count in subscriber_counts:
            for size in message_sizes:
                for num_messages in message_counts:
                    print(f"Testing {pub_count}p/{sub_count}s with {size} bytes, {num_messages} msgs... ", end='', flush=True)
                    result = run_proxy_benchmark(size, num_messages, sub_count, pub_count)
                    print("\r", end='')
                    print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {num_messages:9d} | {result.messages_per_second:11.2f} | "
                          f"{result.latency_ms:11.2f} | {result.message_loss_percent:6.2f} | "
                          f"{result.messages_sent:6d}/{result.expected_sent:<6d} | "
                          f"{result.messages_received:6d}/{result.expected_received:<6d} | "
                          f"{result.memory_mb:10.1f}")
                    time.sleep(1.0)

if __name__ == "__main__":
    main() 