import os
from ether import ether_sub, ether_pub

class DataCollector:
    results = []
    mean = None
    max = None
    min = None

    @ether_sub
    @ether_pub(topic="summarize")
    def collect_result(self, result: int) -> None:
        self.results.append(result)
        print(f"DataCollector[PID {os.getpid()}]: Collected result - {result}")
        

    @ether_sub(topic="summarize")
    def summarize(self) -> None:
        self.mean = sum(self.results) / len(self.results)
        self.max = max(self.results)
        self.min = min(self.results)
        print(f"DataCollector[PID {os.getpid()}]: Summarizing results - mean: {self.mean}, max: {self.max}, min: {self.min}")
