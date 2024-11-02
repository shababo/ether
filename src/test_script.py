from multiprocessing import Process
import zmq

from ether._ether import MyService

def start_service(address: str):
    service = MyService()
    service.receive_messages()

if __name__ == "__main__":
    # Start multiple instances
    processes = []
    for i in range(3):
        p = Process(target=start_service, args=(f"tcp://localhost:5555",))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()

# publisher.py
def publish_message():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")
    
    while True:
        # Send messages...
        pass