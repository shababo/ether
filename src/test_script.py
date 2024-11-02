from multiprocessing import Process, Event
import zmq
import logging
import time
from ether import EtherMixin, ether_pub, ether_sub, get_logger
from typing import Dict, List, Optional, Any
from pydantic import BaseModel

# Define standard ports
BROKER_PUB_PORT = 5555  # Broker publishes on this port
BROKER_SUB_PORT = 5556  # Broker subscribes on this port

class MessageBroker(EtherMixin):
    """Central broker that forwards messages between services"""
    def __init__(self):
        super().__init__(
            name="Broker",
            sub_address=f"tcp://localhost:{BROKER_SUB_PORT}",  # Receive from services
            pub_address=f"tcp://*:{BROKER_PUB_PORT}"          # Publish to services
        )
    
    @ether_sub()  # Subscribe to all messages
    def forward(self, **message):
        """Forward any message received"""
        self._logger.debug(f"Broker forwarding message: {message}")
        # Forward the message to all listeners
        if self._pub_socket:
            self._pub_socket.send_multipart([
                message['topic'].encode(),
                message['payload'].encode()
            ])

class MyService(EtherMixin):
    def __init__(self, process_id: int):
        super().__init__(
            name=f"Service-{process_id}",
            sub_address=f"tcp://localhost:{BROKER_PUB_PORT}",  # Receive from broker
            pub_address=f"tcp://localhost:{BROKER_SUB_PORT}"   # Send to broker
        )
        self.process_id = process_id
    
    @ether_sub()
    def process_data(self, name: str, count: int = 0):
        self._logger.info(f"Processing {name} with count {count}")
        # After processing, publish a result
        self.publish_result(f"Result from {name}", count * 2)
    
    @ether_sub(topic="custom.topic")
    def custom_process(self, data: dict):
        self._logger.info(f"Custom processing: {data}")
    
    @ether_pub(topic="__mp_main__.ResultCollector.collect_result")
    def publish_result(self, result_name: str, result_value: int) -> Dict[str, Any]:
        """Using a simple type hint"""
        self._logger.info(f"Publishing result: {result_name} = {result_value}")
        return {
            "result_name": result_name,  # Match parameter name in collect_result
            "result_value": result_value  # Match parameter name in collect_result
        }

    class ResultData(BaseModel):
        name: str
        value: int
        metadata: Optional[Dict[str, str]] = None

    @ether_pub(topic="__mp_main__.ResultCollector.collect_complex_result")
    def publish_complex_result(self, name: str, value: int) -> ResultData:
        """Using a Pydantic model as return type"""
        return self.ResultData(name=name, value=value, metadata={"source": "calculation"})

    @ether_pub(topic="__mp_main__.ResultCollector.collect_list_result")
    def publish_list_result(self) -> List[int]:
        """Using a list type hint"""
        return [1, 2, 3, 4, 5]

def run_service(stop_event, process_id):
    """Create and run service inside the process"""
    service = MyService(process_id)
    service.run(stop_event)

class ResultCollector(EtherMixin):
    def __init__(self):
        super().__init__(
            name="ResultCollector",
            sub_address="tcp://localhost:5556",  # Subscribe to results
            pub_address=None,  # No publishing needed
        )
        self._logger.debug("ResultCollector initialized")
        
    @ether_sub(topic="__mp_main__.ResultCollector.collect_result")
    def collect_result(self, result_name: str, result_value: int):
        self._logger.info(f"Collected result: {result_name} = {result_value}")
        self._logger.debug("collect_result method called")
    
    @ether_sub()
    def collect_complex_result(self, name: str, value: int, metadata: Optional[Dict[str, str]] = None):
        self._logger.info(f"Collected complex result: {name} = {value} (metadata: {metadata})")
        self._logger.debug("collect_complex_result method called")
    
    @ether_sub()
    def collect_list_result(self, root: List[int]):
        self._logger.info(f"Collected list result: {root}")
        self._logger.debug("collect_list_result method called")

def run_collector(stop_event):
    """Create and run collector inside the process"""
    collector = ResultCollector()
    collector.run(stop_event)

def send_messages():
    logger = get_logger("Publisher")
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")  # Main publisher binds here
    
    logger.info("Publisher started")
    time.sleep(0.15)
    
    topic = "__mp_main__.MyService.process_data"
    logger.info(f"Publishing to topic: {topic}")
    
    for i in range(1):
        logger.info(f"Sending message {i}")
        socket.send_multipart([
            topic.encode(),
            f'{{"name": "test_{i}", "count": {42 + i}}}'.encode()
        ])
        time.sleep(0.1)
    
    logger.info("Publisher finishing")
    socket.close()
    context.term()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main_logger = logging.getLogger("Main")
    
    stop_event = Event()
    
    # Start the result collector process
    main_logger.info("Starting result collector")
    collector_process = Process(target=run_collector, args=(stop_event,))
    collector_process.start()
    
    # Start publisher
    main_logger.info("Starting publisher")
    publisher = Process(target=send_messages)
    publisher.start()
    
    # Start multiple service instances
    processes = []
    for i in range(1):
        main_logger.info(f"Starting service {i}")
        p = Process(target=run_service, args=(stop_event, i))
        p.start()
        processes.append(p)
    
    publisher.join()
    main_logger.info("Publisher finished")
    
    # Give more time for results to be processed and published
    time.sleep(0.1)
    
    main_logger.info("Signaling services to stop")
    stop_event.set()
    
    # Cleanup
    for p in [collector_process] + processes:
        p.join(timeout=5)
        if p.is_alive():
            main_logger.warning(f"Process did not stop gracefully, terminating")
            p.terminate()
        else:
            main_logger.info(f"Process stopped gracefully")

