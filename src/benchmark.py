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
    def __init__(self, message_size: int, port: int):
        super().__init__(
            name=f"Publisher-{message_size}bytes",
            pub_address=f"tcp://localhost:{port}",  # Connect to broker's sub port
            log_level=logging.WARNING
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
    
    @ether_pub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def publish_message(self, timestamp: float) -> Dict:
        """Publish a single message with current timestamp and unique ID"""
        self.message["timestamp"] = timestamp
        self.message["message_id"] = self.message_count
        self.message["sequence"] = self.message_count
        
        # if self.message_count % 10000 == 0:
        #     self._logger.warning(f"Publisher {self.publisher_id} sent {self.message_count} messages")
        
        self.message_count += 1
        return self.message

    def setup_sockets(self):
        """Setup ZMQ sockets with performance tuning options"""
        super().setup_sockets()
        if self._pub_socket:
            # Increase buffer sizes to handle high message rates
            self._pub_socket.setsockopt(zmq.SNDHWM, 1000000)  # ZMQ-level buffer
            self._pub_socket.setsockopt(zmq.SNDBUF, 65536)    # OS-level buffer

class BenchmarkSubscriber(EtherMixin):
    """Subscriber that receives messages and tracks statistics.
    
    Each subscriber maintains its own list of received message IDs and latencies,
    which are written to a file when the subscriber stops.
    """
    def __init__(self, port: int, results_dir: str, subscriber_id: int):
        super().__init__(
            name=f"Subscriber-{subscriber_id}",
            sub_address=f"tcp://localhost:{port}",  # Connect to broker's pub port
            log_level=logging.WARNING
        )
        self.results_file = os.path.join(results_dir, f"subscriber_{subscriber_id}.json")
        self.latencies = []
        self.received_messages = set()
        self.subscriber_id = subscriber_id
        self.publishers = {}  # Dict of publisher_id -> stats
        
        # Add tracking variables
        self.first_message_time = None
        self.last_message_time = None
        self.subscription_time = None
    
    def setup_sockets(self):
        super().setup_sockets()
        if self._sub_socket:
            self._sub_socket.setsockopt(zmq.RCVHWM, 1000000)
            self._sub_socket.setsockopt(zmq.RCVBUF, 65536)
            self.subscription_time = time.time()
            self._logger.debug(f"Subscriber {self.subscriber_id} subscribed at {self.subscription_time}")
    
    @ether_sub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def receive_message(self, data: str, timestamp: float, message_id: int, 
                       publisher_id: str, sequence: int):
        now = time.time()
        
        # Initialize publisher tracking if needed
        if publisher_id not in self.publishers:
            self.publishers[publisher_id] = {
                "sequences": set(),
                "gaps": [],
                "last_sequence": None,
                "first_time": now
            }
        
        pub_stats = self.publishers[publisher_id]
        
        # Track sequence numbers for this publisher
        if pub_stats["last_sequence"] is not None:
            expected = pub_stats["last_sequence"] + 1
            if sequence > expected:
                gap = sequence - expected
                pub_stats["gaps"].append((expected, sequence, gap))
                self._logger.debug(f"Gap from publisher {publisher_id}: "
                                 f"expected {expected}, got {sequence}")
        
        pub_stats["last_sequence"] = sequence
        pub_stats["sequences"].add(sequence)
        
        # Track overall statistics
        latency = (now - timestamp) * 1000
        self.latencies.append(latency)
        self.received_messages.add((publisher_id, sequence))
    
    def run(self, stop_event: Event):
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        self.setup_sockets()
        
        while not stop_event.is_set():
            try:
                if self._sub_socket:
                    self.receive_single_message()
            except Exception as e:
                self._logger.error(f"Error in run loop: {e}")
                break
        
        # Convert sets to lists for JSON serialization
        serializable_publishers = {}
        for pub_id, stats in self.publishers.items():
            serializable_publishers[pub_id] = {
                "sequences": list(stats["sequences"]),  # Convert set to list
                "gaps": stats["gaps"],
                "last_sequence": stats["last_sequence"],
                "first_time": stats["first_time"]
            }
        
        # Save enhanced results
        results = {
            "latencies": self.latencies,
            "publishers": serializable_publishers,  # Use serializable version
            "subscription_time": self.subscription_time,
            "first_message_time": self.first_message_time,
            "last_message_time": self.last_message_time,
            "total_messages": len(self.received_messages),
            "subscriber_id": self.subscriber_id,
            "received_messages": list(self.received_messages)  # Convert set to list
        }
        
        self._logger.debug(f"Subscriber {self.subscriber_id} final stats: "
                          f"received {len(self.received_messages)} messages from "
                          f"{len(self.publishers)} publishers")
        
        with open(self.results_file, 'w') as f:
            json.dump(results, f)
        
        self.cleanup()

class BenchmarkBroker(EtherMixin):
    """Broker that forwards messages from publishers to subscribers.
    
    The broker binds to two ports:
    - One for receiving messages from publishers (SUB socket)
    - One for sending messages to subscribers (PUB socket)
    """
    def __init__(self, pub_port: int, sub_port: int):
        super().__init__(
            name="Broker",
            sub_address=f"tcp://*:{sub_port}",      # BIND for receiving
            pub_address=f"tcp://*:{pub_port}",      # BIND for publishing
            log_level=logging.WARNING
        )
    
    @ether_sub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def receive(self, data: str, timestamp: float, message_id: int, 
                publisher_id: str, sequence: int):
        """Receive message from publisher and forward it"""
        self._logger.debug(f"Broker received message #{sequence} from {publisher_id}")
        self.forward(data, timestamp, message_id, publisher_id, sequence)
    
    @ether_pub(topic="__mp_main__.BenchmarkSubscriber.receive_message")
    def forward(self, data: str, timestamp: float, message_id: int,
                publisher_id: str, sequence: int) -> Dict:
        """Forward received message to all subscribers"""
        self._logger.debug(f"Forwarding message #{sequence} from {publisher_id}")
        return {
            "data": data,
            "timestamp": timestamp,
            "message_id": message_id,
            "publisher_id": publisher_id,  # Include publisher ID
            "sequence": sequence           # Include sequence number
        }

class BenchmarkProxy(EtherMixin):
    """Proxy that uses XPUB/XSUB sockets for efficient message distribution.
    
    XPUB/XSUB sockets are special versions of PUB/SUB that expose subscriptions
    as messages, allowing for proper subscription forwarding.
    """
    def __init__(self, pub_port: int, sub_port: int):
        super().__init__(
            name="Proxy",
            log_level=logging.WARNING
        )
        self.pub_port = pub_port
        self.sub_port = sub_port
        self.frontend = None
        self.backend = None
        self._running = False  # Add flag to control proxy loop
    
    def setup_sockets(self):
        """Setup XPUB/XSUB sockets with optimized settings"""
        self._zmq_context = zmq.Context()
        
        # XSUB socket for receiving from publishers
        self.frontend = self._zmq_context.socket(zmq.XSUB)
        self.frontend.bind(f"tcp://*:{self.sub_port}")
        self.frontend.setsockopt(zmq.RCVHWM, 1000000)
        self.frontend.setsockopt(zmq.RCVBUF, 65536)
        
        # XPUB socket for sending to subscribers
        self.backend = self._zmq_context.socket(zmq.XPUB)
        self.backend.bind(f"tcp://*:{self.pub_port}")
        self.backend.setsockopt(zmq.SNDHWM, 1000000)
        self.backend.setsockopt(zmq.SNDBUF, 65536)
        self.backend.setsockopt(zmq.XPUB_VERBOSE, 1)
        
        # Set TCP keepalive options
        for socket in [self.frontend, self.backend]:
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.IMMEDIATE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 300)
    
    def run(self, stop_event: Event):
        """Run the proxy with graceful shutdown support"""
        def handle_signal(signum, frame):
            stop_event.set()
        
        signal.signal(signal.SIGTERM, handle_signal)
        
        try:
            self.setup_sockets()
            self._running = True
            
            # Create poller to monitor both sockets
            poller = zmq.Poller()
            poller.register(self.frontend, zmq.POLLIN)
            poller.register(self.backend, zmq.POLLIN)
            
            self._logger.warning(f"Starting proxy with {self.frontend} and {self.backend}")  # Log socket details
            
            while self._running and not stop_event.is_set():
                try:
                    events = dict(poller.poll(timeout=100))  # 100ms timeout
                    
                    if self.frontend in events:
                        message = self.frontend.recv_multipart()
                        self._logger.debug(f"Proxy forwarding from frontend: {len(message)} parts")
                        self.backend.send_multipart(message)
                    
                    if self.backend in events:
                        message = self.backend.recv_multipart()
                        self._logger.debug(f"Proxy forwarding from backend: {len(message)} parts")
                        self.frontend.send_multipart(message)
                        
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:  # Timeout, just continue
                        continue
                    else:
                        self._logger.error(f"ZMQ Error in proxy: {e}")
                        raise
                        
        except Exception as e:
            self._logger.error(f"Error in proxy: {e}")
        finally:
            self._running = False
            if self.frontend:
                self.frontend.close()
            if self.backend:
                self.backend.close()
            if self._zmq_context:
                self._zmq_context.term()

# Helper functions to run components in separate processes
def run_subscriber(stop_event: Event, port: int, results_dir: str, subscriber_id: int):
    """Create and run a subscriber in its own process"""
    subscriber = BenchmarkSubscriber(port, results_dir, subscriber_id)
    subscriber.run(stop_event)

def run_broker(stop_event: Event, pub_port: int, sub_port: int):
    """Create and run a broker in its own process"""
    broker = BenchmarkBroker(pub_port, sub_port)
    broker.run(stop_event)

def run_proxy(stop_event: Event, pub_port: int, sub_port: int):
    """Create and run a proxy in its own process"""
    proxy = BenchmarkProxy(pub_port, sub_port)
    proxy.run(stop_event)

def run_broker_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int) -> BenchmarkResult:
    """Run a complete benchmark with specified configuration.
    
    This function:
    1. Starts a broker process
    2. Starts subscriber processes
    3. Creates publishers
    4. Sends messages and measures performance
    5. Collects and aggregates results
    """
    stop_event = Event()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Setup ports
        broker_pub_port = 5555
        broker_sub_port = 5556
        
        # Start broker process
        broker_process = Process(target=run_broker, args=(stop_event, broker_pub_port, broker_sub_port))
        broker_process.start()
        
        # Start subscriber processes
        sub_processes = []
        for i in range(num_subscribers):
            process = Process(target=run_subscriber, args=(stop_event, broker_pub_port, temp_dir, i))
            process.start()
            sub_processes.append(process)
        
        # Create publishers in main process
        publishers = []
        for _ in range(num_publishers):
            publisher = BenchmarkPublisher(message_size, broker_sub_port)
            publisher.setup_sockets()
            publishers.append(publisher)
        
        time.sleep(0.5)  # Wait for all connections to be established
        
        # Monitor system resources
        process = psutil.Process(os.getpid())
        start_time = time.time()
        
        # Send messages from all publishers
        messages_per_publisher = num_messages // num_publishers
        total_messages_sent = 0
        for publisher in publishers:
            for _ in range(messages_per_publisher):
                publisher.publish_message(time.time())
                total_messages_sent += 1
                if num_subscribers > 4:  # Add delay when many subscribers to prevent overload
                    time.sleep(0.0001)  # 100 microseconds
        
        end_time = time.time()
        duration = end_time - start_time
        
        time.sleep(1.0)  # Wait for last messages to be processed
        
        # Stop all processes
        stop_event.set()
        for p in sub_processes + [broker_process]:
            p.join(timeout=5)
        
        # Collect and aggregate results from all subscribers
        all_latencies = []
        publisher_stats = {}  # Track stats per publisher
        
        for i in range(num_subscribers):
            result_file = os.path.join(temp_dir, f"subscriber_{i}.json")
            with open(result_file, 'r') as f:
                results = json.load(f)
                all_latencies.extend(results["latencies"])
                
                # Analyze per-publisher stats
                for pub_id, stats in results["publishers"].items():
                    if pub_id not in publisher_stats:
                        publisher_stats[pub_id] = {
                            "total_messages": 0,
                            "total_gaps": 0,
                            "sequences": set()
                        }
                    pub_stats = publisher_stats[pub_id]
                    pub_stats["sequences"].update(stats["sequences"])
                    pub_stats["total_gaps"] += len(stats["gaps"])
        
        # Log publisher statistics
        for pub_id, stats in publisher_stats.items():
            messages = len(stats["sequences"])
            gaps = stats["total_gaps"]
            self._logger.debug(f"Publisher {pub_id}: {messages} messages, {gaps} gaps")
        
        # Cleanup publishers
        for publisher in publishers:
            publisher.cleanup()
        
        # Calculate final metrics
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
            expected_sent=num_messages * num_publishers,
            messages_received=int(avg_messages_received),
            expected_received=num_messages * num_subscribers
        )

def run_proxy_benchmark(message_size: int, num_messages: int, num_subscribers: int, num_publishers: int) -> BenchmarkResult:
    stop_event = Event()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        proxy_pub_port = 5555
        proxy_sub_port = 5556
        
        # Start proxy process
        proxy_process = Process(target=run_proxy, args=(stop_event, proxy_pub_port, proxy_sub_port))
        proxy_process.start()
        
        # Start subscribers
        sub_processes = []
        for i in range(num_subscribers):
            process = Process(target=run_subscriber, args=(stop_event, proxy_pub_port, temp_dir, i))
            process.start()
            sub_processes.append(process)
        
        # Create publishers
        publishers = []
        for _ in range(num_publishers):
            publisher = BenchmarkPublisher(message_size, proxy_sub_port)
            publisher.setup_sockets()
            publishers.append(publisher)
        
        # Warmup period
        warmup_messages = 1000
        for publisher in publishers:
            for _ in range(warmup_messages):
                publisher.publish_message(time.time())
        time.sleep(0.5)  # Let system stabilize
        
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
    root.setLevel(logging.WARNING)
    
    # Benchmark parameters
    message_sizes = [1000]              # Size of message payload in bytes
    num_messages = 100000                # Total messages to send
    subscriber_counts = [1, 2]    # Number of subscribers to test
    publisher_counts = [2]        # Number of publishers to test
    
    # Run broker benchmark
    # print("\nRunning Broker Pattern Benchmark...")
    # print("Pubs/Subs | Msg Size | Messages/sec | Latency (ms) | Loss % | Received/Sent | Memory (MB)")
    # print("-" * 90)
    
    # for pub_count in publisher_counts:
    #     for sub_count in subscriber_counts:
    #         for size in message_sizes:
    #             print(f"Testing {pub_count}p/{sub_count}s with {size} bytes... ", end='', flush=True)
    #             result = run_broker_benchmark(size, num_messages, sub_count, pub_count)
    #             print("\r", end='')
    #             print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {result.messages_per_second:11.2f} | "
    #                   f"{result.latency_ms:11.2f} | {result.message_loss_percent:6.2f} | "
    #                   f"{result.messages_received:6d}/{result.messages_sent:<6d} | {result.memory_mb:10.1f}")
    
    # Run proxy benchmark
    print("\nRunning XPUB/XSUB Proxy Pattern Benchmark...")
    print("Pubs/Subs | Msg Size | Messages/sec | Latency (ms) | Loss % | Sent/Expected | Received/Expected | Memory (MB)")
    print("-" * 110)
    
    for pub_count in publisher_counts:
        for sub_count in subscriber_counts:
            for size in message_sizes:
                print(f"Testing {pub_count}p/{sub_count}s with {size} bytes... ", end='', flush=True)
                result = run_proxy_benchmark(size, num_messages, sub_count, pub_count)
                print("\r", end='')
                print(f"{pub_count}p/{sub_count:2d}s | {size:8d} | {result.messages_per_second:11.2f} | "
                      f"{result.latency_ms:11.2f} | {result.message_loss_percent:6.2f} | "
                      f"{result.messages_sent:6d}/{result.expected_sent:<6d} | "
                      f"{result.messages_received:6d}/{result.expected_received:<6d} | "
                      f"{result.memory_mb:10.1f}")
                time.sleep(1.0)

if __name__ == "__main__":
    main() 