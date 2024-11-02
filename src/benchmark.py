from multiprocessing import Process, Event
import zmq
import time
import json
import statistics
from dataclasses import dataclass
from typing import List, Dict
import psutil
import os
from ether import EtherMixin, ether_pub, ether_sub, get_logger
import logging

@dataclass
class BenchmarkResult:
    """Holds results of a benchmark run"""
    messages_per_second: float
    latency_ms: float
    cpu_percent: float
    memory_mb: float

class BenchmarkPublisher(EtherMixin):
    def __init__(self, message_size: int, port: int):
        super().__init__(
            name=f"Publisher-{message_size}bytes",
            pub_address=f"tcp://localhost:{port}"
        )
        self._logger.setLevel(logging.INFO)  # Override debug level
        self.message_size = message_size
        # Create a message of specified size
        self.message = {
            "data": "x" * message_size,
            "timestamp": 0
        }
    
    @ether_pub(topic="benchmark")
    def publish_message(self, timestamp: float) -> Dict:
        self.message["timestamp"] = timestamp
        return self.message

class BenchmarkSubscriber(EtherMixin):
    def __init__(self, port: int):
        super().__init__(
            name="Subscriber",
            sub_address=f"tcp://localhost:{port}"
        )
        self._logger.setLevel(logging.INFO)  # Override debug level
        self.latencies = []
    
    @ether_sub(topic="benchmark")
    def receive_message(self, data: str, timestamp: float):
        latency = (time.time() - timestamp) * 1000  # ms
        self.latencies.append(latency)

class BenchmarkBroker(EtherMixin):
    def __init__(self, pub_port: int, sub_port: int):
        super().__init__(
            name="Broker",
            sub_address=f"tcp://localhost:{sub_port}",  # Receive from publishers
            pub_address=f"tcp://*:{pub_port}"          # Send to subscribers
        )
        self._logger.setLevel(logging.INFO)
    
    @ether_sub(topic="benchmark")
    def forward(self, data: str, timestamp: float) -> Dict:
        """Forward messages from publishers to subscribers"""
        return {"data": data, "timestamp": timestamp}

def run_direct_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int, port: int) -> BenchmarkResult:
    stop_event = Event()
    
    # Create a socket for publishers to connect to
    context = zmq.Context()
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{port}")  # One binding socket
    
    # Start subscribers
    subscribers = []
    sub_processes = []
    for i in range(num_subscribers):
        subscriber = BenchmarkSubscriber(port)
        process = Process(target=subscriber.run, args=(stop_event,))
        process.start()
        subscribers.append(subscriber)
        sub_processes.append(process)
    
    # Start publishers
    publishers = []
    for i in range(num_publishers):
        publisher = BenchmarkPublisher(message_size, port)
        publisher.setup_sockets()
        publishers.append(publisher)
    
    # Wait for connections
    time.sleep(0.5)
    
    # Monitor resources
    process = psutil.Process(os.getpid())
    start_time = time.time()
    
    # Send messages from all publishers
    messages_per_publisher = num_messages // num_publishers
    for publisher in publishers:
        for _ in range(messages_per_publisher):
            publisher.publish_message(time.time())
    
    end_time = time.time()
    
    # Calculate results
    duration = end_time - start_time
    total_messages = messages_per_publisher * num_publishers
    messages_per_second = total_messages / duration
    cpu_percent = process.cpu_percent()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    # Stop subscribers
    stop_event.set()
    for p in sub_processes:
        p.join(timeout=5)
    
    # Cleanup publishers
    for publisher in publishers:
        publisher.cleanup()
    
    # Cleanup binding socket
    pub_socket.close()
    context.term()
    
    # Calculate average latency across all subscribers
    all_latencies = []
    for subscriber in subscribers:
        all_latencies.extend(subscriber.latencies)
    avg_latency = statistics.mean(all_latencies) if all_latencies else 0
    
    return BenchmarkResult(
        messages_per_second=messages_per_second,
        latency_ms=avg_latency,
        cpu_percent=cpu_percent,
        memory_mb=memory_mb
    )

def run_broker_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int) -> BenchmarkResult:
    stop_event = Event()
    
    # Setup ports
    broker_pub_port = 5555
    broker_sub_port = 5556
    
    # Start broker
    broker = BenchmarkBroker(broker_pub_port, broker_sub_port)
    broker_process = Process(target=broker.run, args=(stop_event,))
    broker_process.start()
    
    # Start subscribers
    subscribers = []
    sub_processes = []
    for i in range(num_subscribers):
        subscriber = BenchmarkSubscriber(broker_pub_port)
        process = Process(target=subscriber.run, args=(stop_event,))
        process.start()
        subscribers.append(subscriber)
        sub_processes.append(process)
    
    # Start publishers
    publishers = []
    for i in range(num_publishers):
        publisher = BenchmarkPublisher(message_size, broker_sub_port)
        publisher.setup_sockets()
        publishers.append(publisher)
    
    # Wait for connections
    time.sleep(0.5)
    
    # Monitor resources
    process = psutil.Process(os.getpid())
    start_time = time.time()
    
    # Send messages from all publishers
    messages_per_publisher = num_messages // num_publishers
    for publisher in publishers:
        for _ in range(messages_per_publisher):
            publisher.publish_message(time.time())
    
    end_time = time.time()
    
    # Calculate results
    duration = end_time - start_time
    total_messages = messages_per_publisher * num_publishers
    messages_per_second = total_messages / duration
    cpu_percent = process.cpu_percent()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    # Stop all processes
    stop_event.set()
    for p in sub_processes + [broker_process]:
        p.join(timeout=5)
    
    # Cleanup publishers
    for publisher in publishers:
        publisher.cleanup()
    
    # Calculate average latency across all subscribers
    all_latencies = []
    for subscriber in subscribers:
        all_latencies.extend(subscriber.latencies)
    avg_latency = statistics.mean(all_latencies) if all_latencies else 0
    
    return BenchmarkResult(
        messages_per_second=messages_per_second,
        latency_ms=avg_latency,
        cpu_percent=cpu_percent,
        memory_mb=memory_mb
    )

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s'
    )
    
    message_sizes = [1000]  # Simplified for multiple pub/sub test
    num_messages = 10000
    subscriber_counts = [1, 2, 4, 8]
    publisher_counts = [1, 2, 4]
    
    print("\nRunning Direct Connection Pattern Benchmark...")
    print("Pubs/Subs | Msg Size | Messages/sec | Latency (ms) | CPU % | Memory (MB)")
    print("-" * 75)
    
    for pub_count in publisher_counts:
        for sub_count in subscriber_counts:
            for size in message_sizes:
                print(f"Testing {pub_count}p/{sub_count}s with {size} bytes... ", end='', flush=True)
                result = run_direct_benchmark(size, num_messages, sub_count, pub_count, 5555)
                print("\r", end='')  # Clear the testing message
                print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {result.messages_per_second:11.2f} | {result.latency_ms:11.2f} | {result.cpu_percent:5.1f} | {result.memory_mb:10.1f}")
    
    print("\nRunning Broker Pattern Benchmark...")
    print("Pubs/Subs | Msg Size | Messages/sec | Latency (ms) | CPU % | Memory (MB)")
    print("-" * 75)
    
    for pub_count in publisher_counts:
        for sub_count in subscriber_counts:
            for size in message_sizes:
                print(f"Testing {pub_count}p/{sub_count}s with {size} bytes... ", end='', flush=True)
                result = run_broker_benchmark(size, num_messages, sub_count, pub_count)
                print("\r", end='')  # Clear the testing message
                print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {result.messages_per_second:11.2f} | {result.latency_ms:11.2f} | {result.cpu_percent:5.1f} | {result.memory_mb:10.1f}")

if __name__ == "__main__":
    main() 