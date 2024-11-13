import pytest
import redis
import os
import signal
import time
from ether import ether
import multiprocessing
import logging

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

def wait_for_process(process, timeout=10):
    """Wait for a process to complete with timeout"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not process.is_alive():
            return True
        time.sleep(0.1)
    return False

@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Setup test environment at session start"""
    # Use spawn context for better process isolation
    multiprocessing.set_start_method('spawn', force=True)
    
    # Cleanup any existing processes
    try:
        os.system("pkill -f redis-server")
        os.system("pkill -f zmq")
        time.sleep(1)
    except:
        pass

@pytest.fixture(autouse=True)
def cleanup_between_tests():
    """Cleanup Redis and ZMQ resources between tests"""
    # Setup
    yield
    
    # Cleanup after each test
    try:
        # Shutdown Ether
        ether.shutdown()
        
        # Clean Redis
        r = redis.Redis()
        r.flushall()
        r.close()
        
        # Kill any remaining processes
        os.system("pkill -f zmq")
        
        # Give time for cleanup
        time.sleep(0.5)
        
    except Exception as e:
        print(f"Cleanup error: {e}")

@pytest.fixture
def process_runner():
    """Fixture to run and manage test processes"""
    def run_process(target_func, timeout=10):
        process = multiprocessing.Process(target=target_func)
        process.start()
        
        # Wait for process with timeout
        if not wait_for_process(process, timeout):
            process.terminate()
            process.join(1)
            if process.is_alive():
                process.kill()
                process.join()
            pytest.fail(f"Process timed out after {timeout} seconds")
            
        # Check process exit code
        if process.exitcode != 0:
            pytest.fail(f"Process failed with exit code {process.exitcode}")
            
    return run_process

def pytest_sessionstart(session):
    """Clean up any leftover processes before starting tests"""
    # Cleanup any existing ZMQ processes
    try:
        os.system("pkill -f zmq")
    except:
        pass
    time.sleep(1)

def pytest_sessionfinish(session, exitstatus):
    """Cleanup after all tests are done"""
    try:
        # Final cleanup
        ether.shutdown()
        
        # Clean Redis
        r = redis.Redis()
        r.flushall()
        r.close()
        
        # Kill any remaining ZMQ processes
        os.system("pkill -f zmq")
    except:
        pass