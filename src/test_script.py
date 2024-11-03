from multiprocessing import Process, Event
import zmq
import logging
import time
from ether import (
    EtherMixin, EtherPubSubProxy, ether_pub, ether_sub, get_logger,
    ETHER_PUB_PORT, ETHER_SUB_PORT
)
from typing import Dict, List, Optional, Any
from pydantic import BaseModel\

class DataGenerator(EtherMixin):
    def __init__(self, process_id: int):
        super().__init__(
            name=f"DataGenerator-{process_id}",
            pub_address=f"tcp://localhost:{ETHER_PUB_PORT}",
            log_level=logging.INFO
        )
        self.process_id = process_id

    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, count: int = 42) -> Dict[str, Any]:
        return {"name": f"datagenerator_{self.process_id}", "count": count}

class DataProcessor(EtherMixin):
    def __init__(self, process_id: int):
        super().__init__(
            name=f"DataProcessor-{process_id}",
            log_level=logging.INFO
        )
        self.process_id = process_id
    
    @ether_sub()
    @ether_pub(topic="DataCollector.collect_result")
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

    @ether_pub(topic="DataCollector.collect_complex_result")  
    def publish_complex_result(self, name: str, value: int) -> ResultData:
        return self.ResultData(name=name, value=value, metadata={"source": "calculation"})

    @ether_pub(topic="DataCollector.collect_list_result")  
    def publish_list_result(self) -> List[int]:
        return [1, 2, 3, 4, 5]

class DataCollector(EtherMixin):
    def __init__(self):
        super().__init__(
            name="DataCollector",
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

def run_processor(stop_event, process_id):
    service = DataProcessor(process_id)
    service.run(stop_event)

def run_collector(stop_event):
    collector = DataCollector()
    collector.run(stop_event)

def run_generator(stop_event, process_id):
    generator = DataGenerator(process_id)
    # time.sleep(2.0)
    generator.generate_data()

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
    
    topic = "MyService.process_data"  
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
        main_logger.info(f"Starting processor {i}")
        p = Process(target=run_processor, args=(stop_event, i))
        p.start()
        processes.append(p)
    
    # Give time for everything to connect
    time.sleep(0.5)
    
    # Start publisher
    main_logger.info("Starting generator")
    generator_process = Process(target=run_generator, args=(stop_event, 0))
    generator_process.start()
    generator_process.join()
    main_logger.info("Generator finished")
    
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

