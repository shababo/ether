from multiprocessing import Process, Event
import zmq
import time
import json
import statistics
from dataclasses import dataclass
from typing import Dict
import psutil
import os
from ether import EtherMixin, ether_pub, ether_sub, get_logger
import logging
import tempfile

@dataclass
class BenchmarkResult:
    messages_per_second: float
    latency_ms: float
    cpu_percent: float
    memory_mb: float
    message_loss_percent: float
    messages_sent: int
    messages_received: int

class BenchmarkPublisher(EtherMixin):
    def __init__(self, message_size: int, port: int):
        super().__init__(
            name=f"Publisher-{message_size}bytes",
            pub_address=f"tcp://localhost:{port}",  # Connect to broker's sub port
            log_level=logging.WARNING
        )
        self.message_size = message_size
        self.message_count = 0
        self.message = {
            "data": "x" * message_size,
            "timestamp": 0,
            "message_id": 0
        }
    
    @ether_pub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def publish_message(self, timestamp: float) -> Dict:
        self.message["timestamp"] = timestamp
        self.message["message_id"] = self.message_count
        self.message_count += 1
        return self.message

class BenchmarkSubscriber(EtherMixin):
    def __init__(self, port: int, results_dir: str, subscriber_id: int):
        super().__init__(
            name=f"Subscriber-{subscriber_id}",
            sub_address=f"tcp://localhost:{port}",  # Connect to broker's pub port
            log_level=logging.WARNING
        )
        self.results_file = os.path.join(results_dir, f"subscriber_{subscriber_id}.json")
        self.latencies = []
        self.received_messages = set()
    
    @ether_sub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def receive_message(self, data: str, timestamp: float, message_id: int):
        latency = (time.time() - timestamp) * 1000
        self.latencies.append(latency)
        self.received_messages.add(message_id)
    
    def run(self, stop_event: Event):
        super().run(stop_event)
        results = {
            "latencies": self.latencies,
            "received_messages": list(self.received_messages)
        }
        with open(self.results_file, 'w') as f:
            json.dump(results, f)

class BenchmarkBroker(EtherMixin):
    def __init__(self, pub_port: int, sub_port: int):
        super().__init__(
            name="Broker",
            sub_address=f"tcp://*:{sub_port}",      # BIND for receiving
            pub_address=f"tcp://*:{pub_port}",      # BIND for publishing
            log_level=logging.WARNING
        )
    
    @ether_sub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def receive(self, data: str, timestamp: float, message_id: int):
        self._logger.debug(f"Broker received message #{message_id}")
        self.forward(data, timestamp, message_id)
    
    @ether_pub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def forward(self, data: str, timestamp: float, message_id: int) -> Dict:
        self._logger.debug(f"Forwarding message #{message_id}")
        return {
            "data": data,
            "timestamp": timestamp,
            "message_id": message_id
        }

def run_subscriber(stop_event: Event, port: int, results_dir: str, subscriber_id: int):
    subscriber = BenchmarkSubscriber(port, results_dir, subscriber_id)
    subscriber.run(stop_event)

def run_broker(stop_event: Event, pub_port: int, sub_port: int):
    broker = BenchmarkBroker(pub_port, sub_port)
    broker.run(stop_event)

# Direct connection benchmark commented out for now
# def run_direct_benchmark(...):
#     ...

def run_broker_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int) -> BenchmarkResult:
    stop_event = Event()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        broker_pub_port = 5555
        broker_sub_port = 5556
        
        # Start broker
        broker_process = Process(target=run_broker, args=(stop_event, broker_pub_port, broker_sub_port))
        broker_process.start()
        
        # Start subscribers
        sub_processes = []
        for i in range(num_subscribers):
            process = Process(target=run_subscriber, args=(stop_event, broker_pub_port, temp_dir, i))
            process.start()
            sub_processes.append(process)
        
        # Start publishers
        publishers = []
        for _ in range(num_publishers):
            publisher = BenchmarkPublisher(message_size, broker_sub_port)
            publisher.setup_sockets()
            publishers.append(publisher)
        
        time.sleep(0.5)  # Wait for connections
        
        # Monitor resources and run benchmark
        process = psutil.Process(os.getpid())
        start_time = time.time()
        
        messages_per_publisher = num_messages // num_publishers
        total_messages_sent = 0
        for publisher in publishers:
            for _ in range(messages_per_publisher):
                publisher.publish_message(time.time())
                total_messages_sent += 1
        
        end_time = time.time()
        duration = end_time - start_time
        
        time.sleep(1.0)  # Wait for messages to be processed
        
        # Cleanup
        stop_event.set()
        for p in sub_processes + [broker_process]:
            p.join(timeout=5)
        
        # Collect results
        all_latencies = []
        all_received = set()
        for i in range(num_subscribers):
            result_file = os.path.join(temp_dir, f"subscriber_{i}.json")
            with open(result_file, 'r') as f:
                results = json.load(f)
                all_latencies.extend(results["latencies"])
                all_received.update(results["received_messages"])
        
        for publisher in publishers:
            publisher.cleanup()
        
        # Calculate metrics
        total_messages_received = len(all_received)
        avg_messages_received = total_messages_received / num_subscribers
        message_loss_percent = 100 * (1 - avg_messages_received / total_messages_sent)
        
        return BenchmarkResult(
            messages_per_second=total_messages_sent / duration if duration > 0 else 0,
            latency_ms=statistics.mean(all_latencies) if all_latencies else 0,
            cpu_percent=process.cpu_percent(),
            memory_mb=process.memory_info().rss / 1024 / 1024,
            message_loss_percent=message_loss_percent,
            messages_sent=total_messages_sent,
            messages_received=int(avg_messages_received)
        )

def main():
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )
    
    message_sizes = [1000]
    num_messages = 10000
    subscriber_counts = [1, 2, 4, 8]
    publisher_counts = [1, 2, 4]
    
    print("\nRunning Broker Pattern Benchmark...")
    print("Pubs/Subs | Msg Size | Messages/sec | Latency (ms) | Loss % | Received/Sent | Memory (MB)")
    print("-" * 90)
    
    for pub_count in publisher_counts:
        for sub_count in subscriber_counts:
            for size in message_sizes:
                print(f"Testing {pub_count}p/{sub_count}s with {size} bytes... ", end='', flush=True)
                result = run_broker_benchmark(size, num_messages, sub_count, pub_count)
                print("\r", end='')  # Clear the testing message
                print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {result.messages_per_second:11.2f} | "
                      f"{result.latency_ms:11.2f} | {result.message_loss_percent:6.2f} | "
                      f"{result.messages_received:6d}/{result.messages_sent:<6d} | {result.memory_mb:10.1f}")

if __name__ == "__main__":
    main() 