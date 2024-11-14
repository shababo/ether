# Ether

Ether dynamically facilitates local and remote function calling between Python instances and processes while remaining nearly invisible. The primary focus is to help scientists and engineers integrate existing code into flexible data acquisition and analysis systems with minimal overhead.

## Motivation

Scientists and engineers often have existing code that works well for individual tasks but becomes challenging to integrate into larger systems. For example:

- A computational biologist has code that operates lab equipment and runs analysis, and wants to integrate it into an automated data acquisition pipeline
- An AI engineer wants to combine multiple machine learning models into a real-time processing pipeline
- An operations engineer needs to add remote monitoring to existing industrial control systems
- A behavioral scientist wants to turn a Gymnasium environment into an interactive game with joystick control and real-time display

Ether makes these integrations simple by providing a lightweight, decorator and configuration based interface which enables function calling, fast message passing, and optimized data sharing between Python instances running within the same process, in different processes, or even on different machines.

## Features

- Transform existing classes into distributed system components with just decorators
- Automatic process management and monitoring
- Configuration-based instance launching
- Built-in process recovery and cleanup
- Type validation for messages using Pydantic

## Quick Start

Here's an example of how to build a data processing pipeline using Ether. This example shows how three independent classes can work together with minimal modification:

```python
from ether import ether_pub, ether_sub

# Original class - just add decorators to connect it
class DataGenerator:
    @ether_pub(topic="DataProcessor.process_data")
    def generate_data(self, data: int = 42) -> dict:
        print(f"Generating data: {data}")
        return {"name": self.name, "data": data}

# Processing class - can run in a separate process
class DataProcessor:
    def __init__(self, multiplier: int = 2):
        self.multiplier = multiplier
    
    @ether_sub()
    @ether_pub(topic="DataCollector.collect_result")
    def process_data(self, name: str, data: int = 0) -> dict:
        processed_data = data * self.multiplier
        return {
            "result_name": name,
            "value": processed_data
        }

# Collection class - automatically receives results
class DataCollector:
    @ether_sub()
    def collect_result(self, result_name: str, value: int):
        print(f"Collected result: {result_name} = {value}")
```

### Running Components Together

You can run these components either manually in a single process or distributed across multiple processes using a configuration file:

#### Manual Usage
```python
from ether import ether

# Initialize the messaging system
ether.init()

# Create instances as normal
generator = DataGenerator(name="generator1")
processor = DataProcessor(name="processor1", multiplier=2)
collector = DataCollector(name="collector1")

# Use normally - messages flow automatically
generator.generate_data(42)
```

#### Distributed Usage with Configuration

Create a YAML configuration file (`config.yaml`) to specify how components should be distributed:

```yaml
instances:
  generator1:
    class_path: myapp.DataGenerator
  processor1:
    class_path: myapp.DataProcessor
    kwargs:
      multiplier: 2
  processor2:
    class_path: myapp.DataProcessor
    kwargs:
      multiplier: 4
  collector1:
    class_path: myapp.DataCollector
```

Then run your application:

```python
from ether import ether

# Initialize with config - components launch automatically
ether.init(config="config.yaml")

# Send data into the pipeline
ether.pub({"data": 42}, topic="DataGenerator.generate_data")
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

### How It Works

1. The registry configuration specifies:
   - Which classes to modify via the keys in `registry`
   - Which methods to decorate via the keys in `methods`
   - What decorators to apply (e.g. `ether_pub`, `ether_sub`. etc.)
   - The `kwargs` to pass to each decorator

2. When Ether initializes:
   ```python
   from ether import ether
   
   # Load config from file
   ether.init(config="path/to/config.yaml")
   
   # Or use dict configuration
   config = {
       "registry": {
           "my.module.MyClass": {
               "methods": {
                   "my_method": {
                       "ether_pub": {"topic": "my_topic"}
                   }
               }
           }
       }
   }
   ether.init(config=config)
   ```

3. The specified decorators are applied to the classes, and Ether functionality is added
4. The classes can then be used normally, either:
   - Created manually: `instance = MyClass()`
   - Launched automatically via the `instances` configuration

### Benefits

- No modification of source code required
- Works with third-party classes
- Configuration can be changed without code changes
- Same functionality as manual decoration

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

## How It Works

#### **Decorators**: 
- Ether provides decorators that make it easy to publish or subsribe to messaging topics.
- `@ether_pub` and `@ether_sub` provide general decorators that define message types automatically from method inputs and return types.
- Ether also provides shortcut subscribe decorators for common steps in data acqusition and analysis systems (e.g. @save, @cleanup, @startup, @log)
#### **Automatic Process Management**: 
- Ether handles process creation and monitoring
#### **Message Flow**: 
- Messages automatically flow between components based on topics
#### **Type Safety**: 
- Messages are validated using Pydantic models

