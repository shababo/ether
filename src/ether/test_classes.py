from pydantic import BaseModel
from ether import ether_pub, ether_sub

class MyService(BaseModel):
    name: str
    value: int
    
    @ether_sub()
    def process_data(self, data: str):
        print(f"Processing {data}")
    
    @ether_pub(topic="custom.topic")
    def custom_process(self, message: str):
        print(f"Custom processing: {message}")