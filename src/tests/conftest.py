import pytest
import redis
import os
import signal
import time
import multiprocessing
import logging
import subprocess
import tempfile
from pathlib import Path

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_process(process, timeout=600):
    """Wait for a process to complete with timeout"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if not process.is_alive():
            return True
        time.sleep(0.1)
    return False

def ensure_redis_running():
    """Ensure Redis server is running, handling various edge cases"""
    try:
        # Create user-accessible directory for Redis files
        redis_dir = Path(tempfile.gettempdir()) / "ether_test_redis"
        redis_dir.mkdir(exist_ok=True)
        
        # Kill any existing Redis server
        os.system("pkill -f redis-server")
        time.sleep(1)
        
        # Clean up any existing Redis files
        for file in redis_dir.glob("*"):
            file.unlink()
            
        # Create Redis config
        redis_config = {
            'port': 6379,
            'daemonize': 'yes',
            'dir': str(redis_dir),
            'logfile': str(redis_dir / 'redis.log'),
            'pidfile': str(redis_dir / 'redis.pid'),
            'bind': '127.0.0.1',
            'loglevel': 'warning'
        }
        
        config_path = redis_dir / 'redis.conf'
        with open(config_path, 'w') as f:
            for key, value in redis_config.items():
                f.write(f"{key} {value}\n")
        
        # Start Redis with our config
        subprocess.run(['redis-server', str(config_path)], 
                     stderr=subprocess.DEVNULL, 
                     stdout=subprocess.DEVNULL)
        time.sleep(1)
        
        # Test connection
        r = redis.Redis()
        r.ping()
        r.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to start Redis: {e}")
        return False

# @pytest.fixture(scope="session", autouse=True)
# def setup_test_env():
#     """Setup test environment at session start"""
#     # Use spawn context for better process isolation
#     multiprocessing.set_start_method('spawn', force=True)
    
    # Cleanup any existing processes
    # try:
    #     os.system("pkill -f redis-server")
    #     os.system("pkill -f zmq")
    #     time.sleep(1)
    # except:
    #     pass
    
    # Ensure Redis is running
    # if not ensure_redis_running():
    #     pytest.fail("Failed to start Redis server")

# @pytest.fixture(autouse=True)
# def cleanup_between_tests():
#     """Cleanup Redis and ZMQ resources between tests"""
#     # Setup
#     yield
    
#     # Cleanup after each test
#     try:
#         # Shutdown Ether
#         # ether.shutdown()
        
#         # # Clean Redis
#         # r = redis.Redis()
#         # r.flushall()
#         # r.close()
        
#         # Kill any remaining processes
#         # os.system("pkill -f zmq")
        
#         # Give time for cleanup
#         time.sleep(0.5)
        
#     except Exception as e:
#         logger.error(f"Cleanup error: {e}")

@pytest.fixture
def process_runner():
    """Fixture to run and manage test processes"""
    def run_process(target_func, timeout=600):
        process = multiprocessing.Process(target=target_func)
        process.start()
        
        try:
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
                
        finally:
            # Ensure cleanup happens even if test fails
            try:
                # Kill any remaining processes
                # os.system("pkill -f redis-server")
                # os.system("pkill -f zmq")
                
                # Clean Redis files
                redis_dir = Path(tempfile.gettempdir()) / "ether_test_redis"
                if redis_dir.exists():
                    for file in redis_dir.glob("*"):
                        try:
                            file.unlink()
                        except:
                            pass
                    try:
                        redis_dir.rmdir()
                    except:
                        pass
                
                # Give time for cleanup
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error during process cleanup: {e}")
            
    return run_process

# def pytest_sessionstart(session):
#     """Clean up any leftover processes before starting tests"""
#     # Cleanup any existing ZMQ processes
#     try:
#         os.system("pkill -f zmq")
#     except:
#         pass
#     time.sleep(1)

def pytest_sessionfinish(session, exitstatus):
    """Cleanup after all tests are done"""
    try:
        # Final cleanup
        # ether.shutdown()
        
        # Clean Redis
        # r = redis.Redis()
        # r.flushall()
        # r.close()
        
        # Kill any remaining processes
        # os.system("pkill -f zmq")
        # os.system("pkill -f redis-server")
        
        # Clean up Redis directory
        redis_dir = Path(tempfile.gettempdir()) / "ether_test_redis"
        if redis_dir.exists():
            for file in redis_dir.glob("*"):
                try:
                    file.unlink()
                except:
                    pass
            try:
                redis_dir.rmdir()
            except:
                pass
    except:
        pass

@pytest.fixture(scope='session', autouse=True)
def clear_loggers():
    """Remove handlers from all loggers"""
    import logging
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, 'handlers', [])
        for handler in handlers:
            logger.removeHandler(handler)