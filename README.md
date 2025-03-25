<p align="center">
    <img src="https://github.com/user-attachments/assets/dc108544-90d4-42a1-ab06-bfebb3893edf">
</p>

## What is Ether?

Ether is an ultra-lightweight, Python framework for scientists and engineers to integrate the components of their lab.

### User perspective 
A lot of software meant to support science asks a lot of users up front. This is a huge blocker for several reasons. First of all, scientists are already time-constrained. Second, not all users can on board new software, and potentially new computing concepts, rapidly enough. But most importantly, the process of science or engineering, very frequently, involves changing and re-combining components, testing and debugging, and making decisions on the fly. The core goal of Ether is to be useful even under those conditions while asking the least amount possible from the users. 

#### Features

- Integrate your code without changing it, across machines or even the Internet
- Guarantees on data saving and provenance tracing
- Configuration complexity scales with use case complexity
- Quickly run common scientific computing patterns (e.g. data pipelines, automation/scheduling, trial-based experiments)
- No need to learn about computing stuff you don't care about (e.g. schema, servers, async, inheritance)

Ether achieves all of this via two core design choices: 
1. As much as possible, Ether looks at your code for answers. The result is that Ether can dynamically structure logs and metadata when you run experiments! One way we like to put it is that "Ether doesn't ask what you are going to do, it tells you what you did."
2. Ether abstracts all of the complicated stuff behind a simple interface based on semantics close to the user - not the developer.

That said, the best way for a user to understand Ether is to look at use cases. Several use case demos are in the works, but for now here are examples of how you might use it.
#### Example Use Cases
- A computational biologist has code that operates lab equipment and runs analysis, and wants to integrate it into an automated data acquisition pipeline
- An AI engineer wants to combine multiple machine learning models into a real-time processing pipeline
- An operations engineer needs to add remote monitoring to existing industrial control systems
- A neuroscientist wants to run a Gymnasium environment in a behavioral set up while recording physiology

#### Modularity and the Community Hub

Ether's ability to intergrate your code without the need to change it (or at least make any reference to Ether itself in the code) enables two important features.
1. It becomes extremely easy to swap out steps of a data processing pipeline or hardware components.
2. It also becomes easy to share code across projects and organizations. If someone writes code to do some processing or control a piece of hardware, Ether allows other users to integrate that code. This works best when each users write small Python classes that take care of small parts of the system.

### Developer perspective
Ether dynamically facilitates direct, local, and remote function calling between Python instances and processes as well as data management and logging. It is designed to minimize user overhead and coding skill requirements by relying on an interface based on decorators, yaml configurations, and a small number of direct calls to Ether functions. It achieves these goals by wrapping and introspecting the user's code and, in many cases, launching instances of user's classes in their own processes. When instances are run in their own processes, the result is that we've dynamically turned those classes into microservices. 

At its foundation, Ether provides traceability/observability and both Pub-Sub and Request-Reply messaging patterns. It then builds on those using several layers of abstraction, the last of which is a lightweight, user-friendly API which wraps the common function calling patterns in science like triggering data pipelines, automating acquistion hardware into runs/experiments/trials, and storing and retrieving data.

##### Features

- Transform existing classes into distributed system components via decorators and/or yaml configuration
- Automatic session discovery, lifecycle management, and monitoring of processes (locally or over IP)
- Guarantees on data saving and provenance tracing
- System-wide logging of messages, errors, etc.
- Configuration complexity scales with use case complexity
- Session-wide security and encrypted communication (currently in development!)
- Pydantic type validation for messages based on type hints

## Installation
```
pip install ether-medium
```
CircuitPython distribution (Coming soon!)
```
circup install ether-lite 
```

## Quick Start

This example shows how you can easily setup your existing code to work with Ether. This particular example is based on a simple data processing pipeline. 

### Decorating Your Existing Code

In `data_generator.py`:
```python
# Data Generator - produces initial data
class DataGenerator:
    @ether_start()
    @ether_pub(topic="data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"DataGenerator[PID {os.getpid()}]: Generating data - {data}")
        return {"data": data}
```
In `data_processor.py`:
```python
# Data Processor - can run in multiple instances with different multipliers
class DataProcessor:
    def __init__(self, multiplier: int = 2):
        self.multiplier = multiplier
    
    @ether_sub(topic="data")
    @ether_pub(topic="result")
    def process_data(self, data: int = 0) -> dict:
        print(f"DataProcessor[PID {os.getpid()}]: Processing input data - {data}")
        result = data * self.multiplier
        print(f"DataProcessor[PID {os.getpid()}]: Processed output result - {result}")
        return {"result": result}
```

In `data_collector.py`:
```python
# Data Collector - collects and summarizes results
class DataCollector:
    results = []
    
    @ether_sub(topic="result")
    @ether_pub(topic="summarize")
    def collect_result(self, result: int) -> None:
        self.results.append(result)
        print(f"DataCollector[PID {os.getpid()}]: Collected result - {result}")

    @ether_sub(topic="summarize")
    def summarize(self) -> None:
        mean = sum(self.results) / len(self.results)
        max_val = max(self.results)
        min_val = min(self.results)
        print(f"DataCollector[PID {os.getpid()}]: Summarizing results - mean: {mean}, max: {max_val}, min: {min_val}")
```

### Running Components Together

The example below demonstrates two key features of Ether:

1. **Distributed Execution**: Using a configuration file, Ether can automatically launch and coordinate multiple processes, each running different instances of your components. The components communicate through Ether's messaging system, allowing for parallel processing and distributed workloads.

2. **Transparent Fallback**: After Ether shuts down, your classes retain their original behavior and can still be used directly - just as if they were normal Python classes. This means your code remains functional even without Ether running and that any non-Ether usage of your code will retain its expected functionality.

Here's how to run the example in both modes:

#### Distributed Ether Configuration

Create a YAML configuration file in `config/dual_processors.yaml` to specify how components should be distributed:

```yaml
instances:
  generator1:
    class_path: examples.simple_data_processing.DataGenerator
  processor2x:
    class_path: examples.simple_data_processing.DataProcessor
    kwargs:
      multiplier: 2
  processor4x:
    class_path: examples.simple_data_processing.DataProcessor
    kwargs:
      multiplier: 4
  collector1:
    class_path: examples.simple_data_processing.DataCollector
```

Below we run our pipeline, first using Ether:

```python
import os
import time
from ether import ether

# Get path to config file
def _get_config_path(config_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "config", f"{config_name}.yaml")

# Tap in to the ether with config
config_path = _get_config_path("dual_processors")
ether.tap(config=config_path)

# Start the distributed system
ether.start()
time.sleep(1.002)  # Let the system run for a bit

# Shut down the distributed system
ether.shutdown()
```

Then immediately after we can use the same imported classes to run the pipeline without Ether in the main process:
```python
# After shutdown, you can still use components normally in a single process
generator = DataGenerator()
processor2x = DataProcessor(multiplier=2)
processor4x = DataProcessor(multiplier=4)
collector = DataCollector()

# Manual usage example
generated_data = generator.generate_data(data=4242)
processed2x_result = processor2x.process_data(**generated_data)
processed4x_result = processor4x.process_data(**generated_data)
collector.collect_result(**processed2x_result)
collector.collect_result(**processed4x_result)
collector.summarize()
```

Expected output (note that the Ether log PID and the manuage usage PIDs are all the same):
```
2025-01-16 11:28:42.765 - Ether:51475 - INFO - Starting Ether session: 52a3af6a-3ca2-4b88-811d-256612abc6c3...
2025-01-16 11:28:44.331 - Ether:51475 - INFO - Ether system started successfully
DataGenerator[PID 51483]: Generating data - 42
DataProcessor[PID 51485]: Processing input data - 42
DataProcessor[PID 51484]: Processing input data - 42
DataProcessor[PID 51485]: Processed output result - 168
DataProcessor[PID 51484]: Processed output result - 84
DataCollector[PID 51486]: Collected result - 168
DataCollector[PID 51486]: Collected result - 84
DataCollector[PID 51486]: Summarizing results - mean: 126.0, max: 168, min: 84
DataCollector[PID 51486]: Summarizing results - mean: 126.0, max: 168, min: 84
2025-01-16 11:28:45.346 - Ether:51475 - INFO - Shutting down Ether session: 52a3af6a-3ca2-4b88-811d-256612abc6c3...
2025-01-16 11:28:45.448 - Ether:51475 - INFO - Ether system shutdown complete

# Manual usage output after shutdown:
DataGenerator[PID 51475]: Generating data - 4242
DataProcessor[PID 51475]: Processing input data - 4242
DataProcessor[PID 51475]: Processed output result - 8484
DataProcessor[PID 51475]: Processing input data - 4242
DataProcessor[PID 51475]: Processed output result - 16968
DataCollector[PID 51475]: Collected result - 8484
DataCollector[PID 51475]: Collected result - 16968
DataCollector[PID 51475]: Summarizing results - mean: 12726.0, max: 16968, min: 8484
```

## Using Ether with External Classes

Sometimes you may want to use Ether with classes that you can't or don't want to modify directly. Ether provides a way to apply pub/sub decorators through configuration instead of modifying source code.

### Registry Configuration

The registry configuration allows you to specify which methods should be decorated with `ether_pub` and `ether_sub`. Here's an example:
```yaml
registry:
  examples.external_class_integration.DataGenerator:
    methods:
      generate_data:
        ether_sub: {} # Empty dict for default settings
        ether_pub:
          topic: "data" # Publish to 'data' topic
  examples.external_class_integration.DataProcessor:
    methods:
      process_data:
        ether_sub:
          topic: "data" # Subscribe to 'data' topic
        ether_pub:
          topic: "processed_data" # Publish to 'processed_data' topic
  examples.external_class_integration.DataCollector:
    methods:
      collect_result:
        ether_sub:
          topic: "processed_data" # Subscribe to 'processed_data' topic
instances:
  generator:
    class_path: third_party_module.DataGenerator
    kwargs:
      process_id: 1
  processor2x:
    class_path: third_party_module.DataProcessor
    kwargs:
      multiplier: 2
  collector:
    class_path: third_party_module.DataCollector
```

### Benefits

- No modification of source code required
- Works with third-party classes

## Real-World Example: Automated Lab Equipment

Here's how you might use Ether to automate a lab experiment:

```python
class TemperatureSensor:
    @ether_pub(topic="DataLogger.log_temperature")
    def read_temperature(self) -> dict:
        temp = self._hardware.get_temperature()
        return {"temperature": temp, "timestamp": time.time()}

class DataLogger:
    @ether_sub()
    @ether_pub(topic="ExperimentController.check_temperature")
    def log_temperature(self, temperature: float, timestamp: float) -> dict:
        self._db.save_temperature(temperature, timestamp)
        return {"temperature": temperature, "timestamp": timestamp}

class ExperimentController:
    @ether_sub()
    def check_temperature(self, temperature: float, timestamp: float):
        if temperature > self.max_temp:
            self._safety_shutdown()
```


