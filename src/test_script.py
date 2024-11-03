from multiprocessing import Process, Event
import zmq
import logging
import time
from ether import (
    EtherMixin, EtherPubSubProxy, ether_pub, ether_sub, get_logger,
    ETHER_PUB_PORT
)
from typing import Dict, List, Optional, Any
from pydantic import BaseModel

class MyService(EtherMixin):
    def __init__(self, process_id: int):
        super().__init__(
            name=f"Service-{process_id}",
            log_level=logging.INFO
        )
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="ResultCollector.collect_result")
    def process_data(self, name: str, count: int = 0) -> Dict[str, Any]:
        self._logger.info(f"Processing {name} with count {count}")
        # After processing, publish a result
        processed_count = count * 2
        self._logger.info(f"Publishing result: {name} = {count * 2}")
        return {
            "result_name": name,
            "value": processed_count
        }
    
    @ether_sub(topic="custom.topic")
    def custom_process(self, data: dict):
        self._logger.info(f"Custom processing: {data}")

    class ResultData(BaseModel):
        name: str
        value: int
        metadata: Optional[Dict[str, str]] = None

    @ether_pub(topic="ResultCollector.collect_complex_result")  
    def publish_complex_result(self, name: str, value: int) -> ResultData:
        return self.ResultData(name=name, value=value, metadata={"source": "calculation"})

    @ether_pub(topic="ResultCollector.collect_list_result")  
    def publish_list_result(self) -> List[int]:
        return [1, 2, 3, 4, 5]

class ResultCollector(EtherMixin):
    def __init__(self):
        super().__init__(
            name="ResultCollector",
            log_level=logging.INFO
        )
    
    @ether_sub()  
    def collect_result(self, result_name: str, value: int):
        self._logger.info(f"Collected result: {result_name} = {value}")
    
    @ether_sub() 
    def collect_complex_result(self, name: str, value: int, metadata: Optional[Dict[str, str]] = None):
        self._logger.info(f"Collected complex result: {name} = {value} (metadata: {metadata})")
    
    @ether_sub()  
    def collect_list_result(self, root: List[int]):
        self._logger.info(f"Collected list result: {root}")

def run_service(stop_event, process_id):
    service = MyService(process_id)
    service.run(stop_event)

def run_collector(stop_event):
    collector = ResultCollector()
    collector.run(stop_event)

def run_proxy(stop_event):
    proxy = EtherPubSubProxy()
    proxy.run(stop_event)

def send_messages():
    logger = get_logger("Publisher")
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect(f"tcp://localhost:{ETHER_PUB_PORT}")  # Connect to proxy's XSUB
    
    logger.info("Publisher started")
    time.sleep(0.15)  # Wait for connections
    
    topic = "MyService.process_data"  # Removed __mp_main__
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
    
    # Start the proxy
    main_logger.info("Starting proxy")
    proxy_process = Process(target=run_proxy, args=(stop_event,))
    proxy_process.start()
    
    # Start the result collector
    main_logger.info("Starting result collector")
    collector_process = Process(target=run_collector, args=(stop_event,))
    collector_process.start()
    
    # Start service instances
    processes = []
    for i in range(1):
        main_logger.info(f"Starting service {i}")
        p = Process(target=run_service, args=(stop_event, i))
        p.start()
        processes.append(p)
    
    # Give time for everything to connect
    time.sleep(0.5)
    
    # Start publisher
    main_logger.info("Starting publisher")
    publisher = Process(target=send_messages)
    publisher.start()
    
    publisher.join()
    main_logger.info("Publisher finished")
    
    # Give time for results to be processed
    time.sleep(0.5)
    
    # Cleanup
    main_logger.info("Signaling processes to stop")
    stop_event.set()
    
    for p in [proxy_process, collector_process] + processes:
        p.join(timeout=5)
        if p.is_alive():
            main_logger.warning(f"Process did not stop gracefully, terminating")
            p.terminate()
        else:
            main_logger.info(f"Process stopped gracefully")

