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
    REQUEST_DATA_INDEX,
    REPLY_CLIENT_INDEX,
    REPLY_SERVICE_INDEX,
    REPLY_DATA_INDEX,
    REQUEST_WORKER_INDEX,
    REQUEST_COMMAND_INDEX,
    REQUEST_CLIENT_ID_INDEX
)
from ether.utils import get_ether_logger

def run_worker(service_name):
    """Run a worker in a separate process"""
    logger = get_ether_logger("Worker")
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.RCVTIMEO, 2500)
    socket.connect("tcp://localhost:5560")
    
    try:
        # Register with broker
        logger.debug("Registering worker")
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
                assert msg[REQUEST_COMMAND_INDEX] == W_REQUEST
                client_id = msg[REQUEST_CLIENT_ID_INDEX]
                request = json.loads(msg[REQUEST_DATA_INDEX].decode())
                logger.debug(f"Decoded request: {request}")
                
                # Send reply with request ID echoed back
                reply_data = {
                    "result": "success",
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
                
            except zmq.error.Again:
                # Normal timeout, return success
                return True
                
    except Exception as e:
        logger.error(f"Worker error: {e}")
        return False
    finally:
        socket.close()
        context.term()

def run_client(service_name, client_id):
    """Run a client in a separate process"""
    logger = get_ether_logger(f"Client-{client_id}")
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.RCVTIMEO, 2500)
    socket.connect("tcp://localhost:5559")
    
    try:
        replies_received = set()
        
        # Send 5 requests
        for request_id in range(5):
            request_data = {
                "action": "test",
                "request_id": request_id,
                "client_id": client_id
            }
            logger.debug(f"Sending request {request_id}")
            socket.send_multipart([
                b'',
                MDPC_CLIENT,
                service_name,
                json.dumps(request_data).encode()
            ])
            
            # Get reply
            logger.debug(f"Waiting for reply to request {request_id}")
            try:
                msg = socket.recv_multipart()
                logger.debug(f"Received reply: {msg}")
                
                assert msg[REPLY_CLIENT_INDEX] == MDPC_CLIENT
                assert msg[REPLY_SERVICE_INDEX] == service_name
                reply = json.loads(msg[REPLY_DATA_INDEX].decode())
                
                # Verify reply matches our request
                assert reply["result"] == "success"
                assert reply["request_id"] == request_id
                assert reply["client_id"] == client_id
                
                replies_received.add(request_id)
                
            except zmq.error.Again:
                logger.error(f"Timeout waiting for reply to request {request_id}")
                return False
                
        # Verify we got all replies
        assert len(replies_received) == 5, f"Only received {len(replies_received)} replies"
        return True
            
    except Exception as e:
        logger.error(f"Client error: {e}")
        return False
    finally:
        socket.close()
        context.term()

def test_ether_request_reply():
    logger = get_ether_logger("TestEtherReqRep")
    logger.debug("Starting Ether request-reply test")
    
    # Initialize Ether (this starts the broker)
    ether.tap()

    time.sleep(5)
    
    service_name = b"test_service"
    processes = []
    
    try:
        # Allow broker to initialize
        time.sleep(0.5)
        
        # Start worker process
        worker_process = multiprocessing.Process(target=run_worker, args=(service_name,), name="Worker")
        worker_process.daemon = True
        worker_process.start()
        processes.append(worker_process)
        
        # Allow worker to register
        time.sleep(1.0)
        
        # Start multiple client processes
        client_processes = []
        for client_id in range(2):
            client_process = multiprocessing.Process(
                target=run_client, 
                args=(service_name, client_id), 
                name=f"Client-{client_id}"
            )
            client_process.daemon = True
            client_process.start()
            processes.append(client_process)
            client_processes.append(client_process)
        
        # Wait for all clients to complete
        for client_process in client_processes:
            client_process.join(timeout=10)
            assert not client_process.is_alive(), f"Client process {client_process.name} timed out"
            assert client_process.exitcode == 0, f"Client process {client_process.name} failed"
        
        # Allow worker to finish processing
        time.sleep(0.5)
        
        # Signal worker to stop by closing its socket (it will timeout and exit)
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