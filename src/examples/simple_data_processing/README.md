## Simple Data Processing Example

### Distributed Usage with Multiple Data Processing Instances
This example demonstrates a system with a data generator, two data processors, and a result collector.
The generator produces data, which is processed by both processors (with different multipliers)
and then both results are collected by the collector.

### Running the pipeline without Ether
After running the distributed version, Ether is shutdown and the same classes are used normally demonstrating that Ether does not get in the way of any existing usage of your integrated codebase.

### Usage and Output
Config files are located in the config directory relative to this file.

Command line usage:
```(bash)
# no .yaml suffix needed for config_name
# default config_name is dual_processors
python run_dual_processors.py --config-name {dual_processors} 
```

Expected output (ordering of processor and generator outputs may be different):
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
DataGenerator[PID 51475]: Generating data - 4242
DataProcessor[PID 51475]: Processing input data - 4242
DataProcessor[PID 51475]: Processed output result - 8484
DataProcessor[PID 51475]: Processing input data - 4242
DataProcessor[PID 51475]: Processed output result - 16968
DataCollector[PID 51475]: Collected result - 8484
DataCollector[PID 51475]: Collected result - 16968
DataCollector[PID 51475]: Summarizing results - mean: 12726.0, max: 16968, min: 8484
```