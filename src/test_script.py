from multiprocessing import Process
import zmq
import logging
import time
from ether._ether import ZMQReceiverMixin, zmq_method

# Example usage:
class MyService(ZMQReceiverMixin):
    def __init__(self):
        super().__init__(zmq_address="tcp://localhost:5555")
    
    @zmq_method()
    def process_data(self, name: str, count: int = 0):
        logging.info(f"Processing {name} with count {count}")
    
    @zmq_method(topic="custom.topic")
    def custom_process(self, data: dict):
        logging.info(f"Custom processing: {data}")

# Example usage in different processes:

# Process 1 - Service
def run_service():
    service = MyService()
    service.receive_messages()  # Blocks and processes messages

# Process 2 - Publisher
def send_message():

    time.sleep(5)
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")
    
    # Send message to process_data
    socket.send_multipart([
        b"__main__.MyService.process_data",
        b'{"name": "test", "count": 42}'
    ])
    
    # Send message to custom_process
    socket.send_multipart([
        b"custom.topic",
        b'{"data": {"key": "value"}}'
    ])


if __name__ == "__main__":
    # Start multiple instances
    processes = []
    for i in range(3):
        p = Process(target=run_service)
        p.start()
        processes.append(p)

    p = Process(target=send_message)
    p.start()
    processes.append(p)

    for p in processes:
        p.join()

    