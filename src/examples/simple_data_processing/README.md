## Simple Data Processing Example

### Distributed Usage with Multiple Data Processing Instances
This example demonstrates a system with a data generator, two data processors, and a result collector.
The generator produces data, which is processed by both processors (with different multipliers)
and then both results are collected by the collector.

### Running the pipeline without Ether
After running the ether facilitated distributed version. Ether is shutdown and the same classes are used normally, demonstrating that Ether does not get in the way of any existing usage of your integrated codebase.

### Usage and Output
Config files are located in the config directory relative to this file.

Command line usage:
```(bash)
# no .yaml suffix needed for config_name
# default config_name is dual_processors
python main.py --config-name {dual_processors} 
```

Expected output (ordering of processor and generator outputs may be different):
```
2025-01-16 10:28:30.814 - Ether:47841 - INFO - Starting Ether session: d6322dbb-8d3f-4064-bb4a-645bcb355b06...
2025-01-16 10:28:32.371 - Ether:47841 - INFO - Ether system started successfully
Ether system initialized
(PID: 47857)::DataGenerator: Generating data: 42
(PID: 47858)::Processing data value: 42 from DataGenerator
(PID: 47859)::Processing data value: 42 from DataGenerator
(PID: 47860)::Collected result from DataGenerator_4x = 168
(PID: 47860)::Collected result from DataGenerator_2x = 84
2025-01-16 10:28:33.382 - Ether:47841 - INFO - Shutting down Ether session: d6322dbb-8d3f-4064-bb4a-645bcb355b06...
2025-01-16 10:28:33.488 - Ether:47841 - INFO - Ether system shutdown complete
(PID: 47841)::generator_within_process: Generating data: 4242
(PID: 47841)::Processing data value: 4242 from generator_within_process
(PID: 47841)::Processing data value: 4242 from generator_within_process
(PID: 47841)::Collected result from generator_within_process_2x = 8484
(PID: 47841)::Collected result from generator_within_process_4x = 16968
```