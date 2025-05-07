<p align="center">
    <img src="https://github.com/user-attachments/assets/dc108544-90d4-42a1-ab06-bfebb3893edf">
</p>

## What is Ether?

Ether is a user-friendly and low overhead framework for integrating and orchestrating your Python processes. It is specificaly designed to support research and development, where systems are small to medium scale and users are frequently changing and re-combining components, testing and debugging, and making decisions on the fly.

### Example Use Cases
Several demos are in the works, but for now here are some example use cases:
- A computational biologist has code that operates lab equipment and runs analysis, and wants to integrate it into an automated data acquisition pipeline
- An AI engineer wants to combine multiple machine learning models into a real-time processing pipeline
- An operations engineer needs to add remote monitoring to existing industrial control systems
- A neuroscientist wants to run a Gymnasium environment in a behavioral set up while recording physiology

### Features

- Integrate your code with minimal or no changes to your code itself
- Automatic session discovery across machines or the Internet
- Guarantees on data saving and provenance
- Automatic logging and traceability
- Configuration complexity scales with use case complexity
- Quickly run common scientific computing patterns (e.g. data pipelines, automation/scheduling, trial-based experiments)
- No need to learn about computing stuff you don't care about (e.g. schema, servers, async, inheritance)

## Alternative description for developers

Ether dynamically facilitates direct, local, and remote function calling between Python instances and processes as well as data management and logging. It is designed to minimize user overhead and coding skill requirements by relying on an interface based on decorators, yaml configurations, and a small number of direct calls to Ether functions. It achieves these goals by wrapping and introspecting the user's code and, in many cases, launching instances of user's classes in their own processes. When instances are run in their own processes, the result is that we've dynamically turned the tagged methods of those classes into microservices. 

At its foundation, Ether provides traceability/observability and both Pub-Sub and Request-Reply messaging patterns. It then builds on those using several layers of abstraction, the last of which is a lightweight, user-friendly API which allows users to implement common scientific patterns like triggering data pipelines, automating acquistion hardware into runs/experiments/trials, and storing or retrieving data.

#### Features

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

## Quick Start

This example shows how you can easily setup a simple data processing pipeline to work with Ether's pub/sub decorators.

### Adding decorators

The `@ether_sub` decorator listens on a topic (defualt is `{class_name}.{method_name}`) and maps the incoming message to the method's arguments. The `@ether_pub` decorator publishes the method's result to the given topic. We also use `@ether_start()` here which is just short for `@ether_sub(topic="start")`. We can publish to the `start` topic using the call `ether.start()`.

In `data_generator.py`:
```python
class DataGenerator:
    @ether_start() 
    @ether_pub(topic="data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"DataGenerator[PID {os.getpid()}]: Generating data - {data}")
        return {"data": data}
```
In `data_processor.py`:
```python
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

1. **Distributed Execution**: Using a configuration file, Ether can automatically launch and coordinate instances of your components. The components run in parallel and communicate through Ether's messaging system.

2. **Transparent Fallback**: After Ether shuts down, your classes retain their original behavior and can still be used directly - just as if they were normal Python classes. This means your code remains functional even without Ether running and that any non-Ether usage of your code will retain its expected functionality. An important consequence of this is that the process on integrating or removing Ether from a codebase is more robust.

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
time.sleep(1.0)  # Let the system run for a bit

# Shut down the distributed system
ether.shutdown()
```

After Ether shuts down, we can still use the imported classes to run the pipeline without Ether:
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

Expected output (note that the Ether log PID and the manuage usage PIDs are all the same, i.e. the main process):
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

## Other Ether functionality

### Get and Save

The pub/sub decorators used in the example above don't send messages back from subscribers to publishers. If we a message requires a reply, we can use Ether's get and save interface. Below is an example using get and save to implement a simpmle data service.

```python
from typing import List
from ether import ether
from ether.config import EtherConfig, EtherInstanceConfig
from ether import ether_save, ether_get
from ether.utils import get_ether_logger

class DataService:
    def __init__(self):
        self.data = {}
    
    @ether_save()
    def save_item(self, id: int, name: str, tags: List[str]) -> dict:
        """Save an item to the data store"""
        if id in self.data:
            raise ValueError(f"Item {id} already exists")
            
        self.data[id] = {
            "name": name,
            "tags": tags
        }
        return self.data[id]  # Return saved item

    @ether_get
    def get_item(self, id: int) -> dict:
        """Get a single item by ID"""
        if id not in self.data:
            raise KeyError(f"Item {id} not found")
        return self.data[id]

if __name__ == "__main__":
    logger = get_ether_logger("EtherDataServiceMain")
    
    config = EtherConfig(
        instances={
            "data_service": EtherInstanceConfig(
                class_path="test_ether_save.DataService",
                kwargs={"ether_name": "data_service"}
            )
        }
    )
    
    ether.tap(config=config)
    
    try:
        
        # Test saving new item
        reply = ether.save(
            "DataService", 
            "save_item", 
            params={
                "id": 1,
                "name": "Test Item",
                "tags": ["test", "new"]
            },
        )
        logger.info(f"Save reply: {reply}")
        assert reply["status"] == "success"
        result = reply["result"]
        assert result["name"] == "Test Item"
        
        # Verify item was saved
        item = ether.get("DataService", "get_item", params={"id": 1})
        logger.info(f"Get item: {item}")
        assert item["name"] == "Test Item"
        assert item["tags"] == ["test", "new"]
        
        # Test saving duplicate item

        reply = ether.save(
            "DataService", 
            "save_item", 
            params={
                "id": 1,
                "name": "Duplicate",
                "tags": []
            },
        )
        logger.info(f"Duplicate save reply: {reply}")
        assert reply["status"] == "error"
        assert "already exists" in str(reply["error"])
        
    finally:
        ether.shutdown()
```
#### Getting results later
We can also collect the result later (WIP).

```python
req_id = ether.get_later('my_service',...)

response = ether.collect(req_id)
```

### Synchronizing components
Let's say you need a bunch of hardware to initialize or enter a state before running a trial or automation pipeline. In this case you can do coordinate components using `ether.sync`. 
```python
class HardwareComponentA:

    @ether.sync(topic='prepare_trial)
    def set_state(config):
        ...

class HardwareComponentB:

    @ether.sync(topic='prepare_trial')
    def set_state(config):
        ...

# this call will wait until all subscribers to the sync topic have finished
ether.sync('prepare_trial')

run_trial()
```

### Using Ether with External Classes

You may want to use Ether with classes that you can't or don't want to modify directly. Ether provides a way to apply decorators through configuration instead of modifying source code.

#### Registry Configuration

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

#### Benefits

- No modification of source code required
- Works with third-party classes

## Another Example: Automated Lab Monitoring

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


