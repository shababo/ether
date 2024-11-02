from multiprocessing import Process, Event
import zmq
import logging
import time
from ether import ZMQReceiverMixin, zmq_method, get_logger


class MyService(ZMQReceiverMixin):
    def __init__(self):
        super().__init__(zmq_address="tcp://localhost:5555")
    
    @zmq_method()
    def process_data(self, name: str, count: int = 0):
        self._logger.info(f"Processing {name} with count {count}")
    
    @zmq_method(topic="custom.topic")
    def custom_process(self, data: dict):
        self._logger.info(f"Custom processing: {data}")

def service_process(stop_event, process_id):
    logger = get_logger(f"Service-{process_id}")
    service = MyService()
    logger.info(f"Service {process_id} started")
    service.run(stop_event)

def send_message():
    logger = get_logger("Publisher")
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")
    
    logger.info("Publisher started")
    # Minimum delay needed for subscribers to connect
    time.sleep(0.5)  
    
    topic = "__mp_main__.MyService.process_data"
    logger.info(f"Publishing to topic: {topic}")
    
    for i in range(3):
        logger.info(f"Sending message {i}")
        socket.send_multipart([
            topic.encode(),
            f'{{"name": "test_{i}", "count": {42 + i}}}'.encode()
        ])
        time.sleep(0.1)  # Small delay between messages just to avoid flooding
    
    logger.info("Publisher finishing")
    socket.close()
    context.term()

if __name__ == "__main__":
    # Set up logging for the main process
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s'
    )
    main_logger = logging.getLogger("Main")
    
    stop_event = Event()
    
    # Start publisher first
    main_logger.info("Starting publisher")
    publisher = Process(target=send_message)
    publisher.start()
    
    # Start multiple service instances
    processes = []
    for i in range(3):
        main_logger.info(f"Starting service {i}")
        p = Process(target=service_process, args=(stop_event, i))
        p.start()
        processes.append(p)
    
    # Wait for publisher to finish
    publisher.join()
    main_logger.info("Publisher finished")
    
    # Reduced cleanup delay
    time.sleep(1.0)
    
    main_logger.info("Signaling services to stop")
    stop_event.set()
    
    # Wait for services to clean up
    for i, p in enumerate(processes):
        p.join(timeout=5)
        if p.is_alive():
            main_logger.warning(f"Service {i} did not stop gracefully, terminating")
            p.terminate()
        else:
            main_logger.info(f"Service {i} stopped gracefully")