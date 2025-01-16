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
2025-01-16 11:09:11.150 - Ether:50274 - INFO - Starting Ether session: 2a12ac7d-ad67-44e8-af00-e1214a7461ce...
2025-01-16 11:09:12.708 - Ether:50274 - INFO - Ether system started successfully
DataGenerator[PID 50284]: Generating data - 42
DataProcessor[PID 50285]: Processing input data - 42
DataProcessor[PID 50285]: Processed output result - 84
DataProcessor[PID 50286]: Processing input data - 42
DataProcessor[PID 50286]: Processed output result - 168
DataCollector[PID 50287]: Collected result - 84
DataCollector[PID 50287]: Collected result - 168
2025-01-16 11:09:13.721 - Ether:50274 - INFO - Shutting down Ether session: 2a12ac7d-ad67-44e8-af00-e1214a7461ce...
2025-01-16 11:09:13.829 - Ether:50274 - INFO - Ether system shutdown complete
DataGenerator[PID 50274]: Generating data - 4242
DataProcessor[PID 50274]: Processing input data - 4242
DataProcessor[PID 50274]: Processed output result - 8484
DataProcessor[PID 50274]: Processing input data - 4242
DataProcessor[PID 50274]: Processed output result - 16968
DataCollector[PID 50274]: Collected result - 8484
DataCollector[PID 50274]: Collected result - 16968
```