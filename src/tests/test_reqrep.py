import pytest
import zmq
import multiprocessing
import time
import json
from ether._internal._reqrep import EtherReqRepBroker, WORKER_READY, REQUEST, REPLY
from ether.utils import _get_logger

def run_broker():
    """Run the broker in a separate process"""
    broker = EtherReqRepBroker(frontend_port=5559, backend_port=5560)
    try:
        broker.run()
    except KeyboardInterrupt:
        pass
    finally:
        broker.cleanup()

def test_basic_request_reply():
    logger = _get_logger("TestReqRep")
    logger.debug("Starting request-reply test")
    
    # Start broker in separate process
    broker_process = multiprocessing.Process(target=run_broker)
    broker_process.daemon = True
    broker_process.start()
    
    try:
        # Allow broker to initialize
        time.sleep(0.1)
        logger.debug("Setting up worker socket")
        
        # Setup worker with timeout
        worker_context = zmq.Context()
        worker_socket = worker_context.socket(zmq.DEALER)
        worker_socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
        worker_socket.connect("tcp://localhost:5560")
        
        # Worker registers with broker
        service_name = b"test_service"
        logger.debug("Registering worker")
        worker_socket.send_multipart([b'', WORKER_READY, service_name])
        
        # Setup client with timeout
        logger.debug("Setting up client socket")
        client_context = zmq.Context()
        client_socket = client_context.socket(zmq.DEALER)
        client_socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout
        client_socket.connect("tcp://localhost:5559")
        
        # Client sends request
        request_data = {"action": "test"}
        logger.debug("Client sending request")
        client_socket.send_multipart([
            b'',
            service_name,
            json.dumps(request_data).encode()
        ])
        
        # Worker receives request
        logger.debug("Worker waiting for request")
        try:
            worker_msg = worker_socket.recv_multipart()
            logger.debug(f"Worker received message: {worker_msg}")
            assert worker_msg[1] == REQUEST
            client_id = worker_msg[2]
            request = json.loads(worker_msg[3])
            assert request == request_data
        except zmq.error.Again:
            pytest.fail("Timeout waiting for worker to receive request")
        
        # Worker sends reply
        reply_data = {"result": "success"}
        logger.debug("Worker sending reply")
        worker_socket.send_multipart([
            b'',
            REPLY,
            json.dumps(reply_data).encode(),
            client_id
        ])
        
        # Client receives reply
        logger.debug("Client waiting for reply")
        try:
            client_msg = client_socket.recv_multipart()
            logger.debug(f"Client received message: {client_msg}")
            reply = json.loads(client_msg[2])
            assert reply == reply_data
        except zmq.error.Again:
            pytest.fail("Timeout waiting for client to receive reply")
        
    finally:
        logger.debug("Cleaning up test resources")
        # Cleanup
        if 'worker_socket' in locals():
            worker_socket.close()
        if 'client_socket' in locals():
            client_socket.close()
        if 'worker_context' in locals():
            worker_context.term()
        if 'client_context' in locals():
            client_context.term()
            
        # Terminate broker process
        logger.debug("Terminating broker process")
        broker_process.terminate()
        broker_process.join(timeout=1)
        if broker_process.is_alive():
            logger.warning("Broker didn't terminate, killing process")
            broker_process.kill()
            broker_process.join(timeout=1)

if __name__ == '__main__':
    pytest.main([__file__]) 