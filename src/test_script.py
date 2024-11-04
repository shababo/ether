import subprocess
import time
import signal
import os
import logging
import sys
from ether import ether_init
from threading import Thread

def main():
    # Setup root logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )
    logger = logging.getLogger("Main")
    
    # Initialize Ether
    ether_init()
    
    # Add src directory to Python path
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    python_path = os.environ.get('PYTHONPATH', '')
    os.environ['PYTHONPATH'] = f"{src_dir}:{python_path}" if python_path else src_dir
    
    # Set up environment for subprocesses
    subprocess_env = os.environ.copy()
    subprocess_env['PYTHONUNBUFFERED'] = '1'  # Ensure Python output is unbuffered
    subprocess_env['LOGLEVEL'] = 'DEBUG'  # Pass logging level to subprocesses
    logger.info(f"Subprocess environment: LOGLEVEL={subprocess_env.get('LOGLEVEL')}")
    
    # Get absolute paths to scripts
    scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
    
    processes = []
    
    # Start collector with output capture
    logger.info("Starting collector")
    collector = subprocess.Popen(
        [sys.executable, os.path.join(scripts_dir, 'run_collector.py')],
        env=subprocess_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1  # Line buffered
    )
    processes.append(('Collector', collector))
    
    # Start processor with output capture
    logger.info("Starting processor")
    processor = subprocess.Popen(
        [sys.executable, os.path.join(scripts_dir, 'run_processor.py'), '0'],
        env=subprocess_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1  # Line buffered
    )
    processes.append(('Processor', processor))
    
    # Create output threads
    def log_output(process_name, process):
        try:
            logger.info(f"Started output thread for {process_name}")
            for line in process.stdout:
                logger.info(f"{process_name}: {line.strip()}")
        except Exception as e:
            logger.error(f"Error reading {process_name} output: {e}")
    
    # Start output threads
    output_threads = []
    for name, proc in processes:
        thread = Thread(target=log_output, args=(name, proc), daemon=True)
        thread.start()
        output_threads.append(thread)
    
    # Wait for services to start
    time.sleep(1.0)
    
    # Run generator
    logger.info("Running generator")
    generator = subprocess.Popen(
        [sys.executable, os.path.join(scripts_dir, 'run_generator.py'), '0'],
        env=subprocess_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1  # Line buffered
    )
    gen_thread = Thread(target=log_output, args=('Generator', generator), daemon=True)
    gen_thread.start()
    generator.wait()
    
    # Wait for processing
    time.sleep(1.0)
    
    # Cleanup
    logger.info("Stopping services")
    for name, p in processes:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning(f"Process {name} did not stop gracefully, killing")
            p.kill()

if __name__ == "__main__":
    main()

