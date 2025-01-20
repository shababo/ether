import pytest
import zmq
import multiprocessing
import time
import json
from ether import ether
from ether._internal._reqrep import (
    MDPC_CLIENT, 
    MDPW_WORKER,
    W_READY,
    W_REQUEST, 
    W_REPLY,
    REQUEST_WORKER_INDEX,
    REQUEST_COMMAND_INDEX,
    REQUEST_CLIENT_ID_INDEX,
    REQUEST_DATA_INDEX,
    REPLY_CLIENT_INDEX,
    REPLY_SERVICE_INDEX,
    REPLY_DATA_INDEX,
    W_HEARTBEAT
)
from ether.utils import get_ether_logger

def run_worker(service_name):
    """Run a worker in a separate process"""
    logger = get_ether_logger(f"Worker-{service_name.decode()}")
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.linger = 0
    socket.setsockopt(zmq.RCVTIMEO, 2500)
    socket.connect("tcp://localhost:5560")
    
    try:
        # Register with broker
        logger.debug(f"Registering worker for service {service_name}")
        socket.send_multipart([
            b'',
            MDPW_WORKER,
            W_READY,
            service_name
        ])
        
        # Process multiple requests
        while True:
            try:
                msg = socket.recv_multipart()
                logger.debug(f"Received request: {msg}")
                
                assert msg[REQUEST_WORKER_INDEX] == MDPW_WORKER
                command = msg[REQUEST_COMMAND_INDEX]
                
                if command == W_REQUEST:
                    client_id = msg[REQUEST_CLIENT_ID_INDEX]
                    request = json.loads(msg[REQUEST_DATA_INDEX].decode())
                    logger.debug(f"Decoded request: {request}")
                    
                    # Send reply with request details and service name
                    reply_data = {
                        "result": "success",
                        "service": service_name.decode(),
                        "request_id": request["request_id"],
                        "client_id": request["client_id"]
                    }
                    logger.debug(f"Sending reply: {reply_data}")
                    socket.send_multipart([
                        b'',
                        MDPW_WORKER,
                        W_REPLY,
                        service_name,
                        client_id,
                        json.dumps(reply_data).encode()
                    ])
                elif command == W_HEARTBEAT:
                    # Just acknowledge heartbeat by doing nothing
                    logger.debug("Received heartbeat")
                    continue
                
            except zmq.error.Again:
                # Normal timeout, return success
                return True
                
    except Exception as e:
        logger.error(f"Worker error: {e}")
        raise e
    finally:
        socket.close()
        context.term()

def run_client(client_id, services):
    """Run a client that makes requests to multiple services"""
    logger = get_ether_logger(f"Client-{client_id}")
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.RCVTIMEO, 2500)
    socket.connect("tcp://localhost:5559")
    
    try:
        replies_received = {service: set() for service in services}
        
        # Send 3 requests to each service
        for request_id in range(3):
            for service_name in services:
                request_data = {
                    "action": "test",
                    "request_id": request_id,
                    "client_id": client_id,
                    "service": service_name.decode()
                }
                logger.debug(f"Sending request {request_id} to service {service_name}")
                socket.send_multipart([
                    b'',
                    MDPC_CLIENT,
                    service_name,
                    json.dumps(request_data).encode()
                ])
                
                # Get reply
                logger.debug(f"Waiting for reply from {service_name} for request {request_id}")
                try:
                    msg = socket.recv_multipart()
                    logger.debug(f"Received reply: {msg}")
                    
                    assert msg[REPLY_CLIENT_INDEX] == MDPC_CLIENT
                    assert msg[REPLY_SERVICE_INDEX] == service_name
                    reply = json.loads(msg[REPLY_DATA_INDEX].decode())
                    
                    # Verify reply matches our request
                    assert reply["result"] == "success"
                    assert reply["service"] == service_name.decode()
                    assert reply["request_id"] == request_id
                    assert reply["client_id"] == client_id
                    
                    replies_received[service_name].add(request_id)
                    
                except zmq.error.Again:
                    logger.error(f"Timeout waiting for reply from {service_name} to request {request_id}")
                    return False
        
        # Verify we got all replies from all services
        for service_name, replies in replies_received.items():
            assert len(replies) == 3, f"Only received {len(replies)} replies from {service_name}"
        return True
            
    except Exception as e:
        logger.error(f"Client error: {e}")
        return False
    finally:
        socket.close()
        context.term()

def test_multiple_services():
    logger = get_ether_logger("TestMultiService")
    logger.debug("Starting multi-service test")
    
    # Initialize Ether (this starts the broker)
    ether.tap()
    
    services = [b"service1", b"service2"]
    processes = []
    
    try:
        # Allow broker to initialize
        time.sleep(0.5)
        
        # Start worker processes for each service
        for service_name in services:
            worker_process = multiprocessing.Process(
                target=run_worker, 
                args=(service_name,),
                name=f"Worker-{service_name.decode()}"
            )
            worker_process.daemon = True
            worker_process.start()
            processes.append(worker_process)
        
        # Allow workers to register
        time.sleep(1.0)
        
        # Start multiple client processes
        client_processes = []
        for client_id in range(2):
            client_process = multiprocessing.Process(
                target=run_client, 
                args=(client_id, services),
                name=f"Client-{client_id}"
            )
            client_process.daemon = True
            client_process.start()
            processes.append(client_process)
            client_processes.append(client_process)
        
        # Wait for all clients to complete
        for client_process in client_processes:
            client_process.join(timeout=20)  # Increased timeout for multiple services
            assert not client_process.is_alive(), f"Client process {client_process.name} timed out"
            assert client_process.exitcode == 0, f"Client process {client_process.name} failed"
        
        # Allow workers to finish processing
        time.sleep(0.5)
        
        # Signal workers to stop
        for worker_process in processes[:2]:  # First two processes are workers
            worker_process.join(timeout=2)
            if worker_process.is_alive():
                worker_process.terminate()
                worker_process.join(timeout=1)
        
    finally:
        logger.debug("Cleaning up test resources")
        
        # Terminate all processes
        for p in processes:
            if p.is_alive():
                logger.debug(f"Terminating {p.name}")
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    logger.warning(f"Process {p.name} didn't terminate, killing")
                    p.kill()
                    p.join(timeout=1)

        ether.shutdown()

if __name__ == '__main__':
    pytest.main([__file__]) 