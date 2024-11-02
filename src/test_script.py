from multiprocessing import Process, Event
import zmq
import logging
import time
from ether import EtherMixin, ether_pub, ether_sub, get_logger
from typing import Dict, List, Optional
from pydantic import BaseModel


class MyService(EtherMixin):
    def __init__(self, process_id: int):
        # Now we connect for both pub and sub
        super().__init__(
            name=f"Service-{process_id}",
            sub_address="tcp://localhost:5555",  # Subscribe to main messages
            pub_address="tcp://localhost:5556"   # Connect (not bind) for publishing
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
    def publish_result(self, result_name: str, result_value: int) -> Dict[str, int]:
        """Using a simple type hint"""
        self._logger.info(f"Publishing result: {result_name} = {result_value}")
        return {result_name: result_value}

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

def service_process(stop_event, process_id):
    logger = get_logger(f"Service-{process_id}")
    service = MyService(process_id)
    logger.info(f"Service {process_id} started")
    service.run(stop_event)

class ResultCollector(EtherMixin):
    def __init__(self):
        super().__init__(
            name="ResultCollector",
            sub_address="tcp://localhost:5556",  # Subscribe to results
            pub_address=None  # No publishing needed
        )
        
    @ether_sub()
    def collect_result(self, result_name: str, result_value: int):
        self._logger.info(f"Collected result: {result_name} = {result_value}")
    
    @ether_sub()
    def collect_complex_result(self, name: str, value: int, metadata: Optional[Dict[str, str]] = None):
        self._logger.info(f"Collected complex result: {name} = {value} (metadata: {metadata})")
    
    @ether_sub()
    def collect_list_result(self, root: List[int]):
        self._logger.info(f"Collected list result: {root}")


def setup_result_collector():
    """Setup a collector for results from all services"""
    logger = get_logger("ResultCollector")
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5556")  # Only one bind, all services connect here
    return context, socket


def send_messages():
    logger = get_logger("Publisher")
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")  # Main publisher binds here
    
    logger.info("Publisher started")
    time.sleep(0.5)
    
    topic = "__mp_main__.MyService.process_data"
    logger.info(f"Publishing to topic: {topic}")
    
    for i in range(3):
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
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s'
    )
    main_logger = logging.getLogger("Main")
    
    stop_event = Event()

    
    # Start the result collector process
    main_logger.info("Starting result collector")
    collector = ResultCollector()
    collector_process = Process(target=collector.run, args=(stop_event,))
    collector_process.start()
    
    # Start publisher
    main_logger.info("Starting publisher")
    publisher = Process(target=send_messages)
    publisher.start()
    
    # Start multiple service instances
    processes = []
    for i in range(3):
        main_logger.info(f"Starting service {i}")
        p = Process(target=service_process, args=(stop_event, i))
        p.start()
        processes.append(p)
    
    publisher.join()
    main_logger.info("Publisher finished")
    
    time.sleep(2.0)
    
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
    
