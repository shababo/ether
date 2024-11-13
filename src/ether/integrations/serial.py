import serial

from ether import ether_init, ether_sub, ether_pub, ether_cleanup

class SimpleSerial:

    def __init__(self, port: str, baudrate: int = 9600, timeout: float = 1.0):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout

    @ether_init()
    def initialize(self, timeout: float = None):
        timeout = timeout or self.timeout
        self.ser = serial.Serial(self.port, self.baudrate, timeout=timeout)

    @ether_sub(topic="serial_write")
    def write(self, data: str):
        self.ser.write(data.encode())

    @ether_sub(topic="serial_read")
    @ether_pub(topic="serial_response")
    def read(self) -> str:
        return self.ser.readline().decode()
    
    @ether_cleanup()
    def cleanup(self):
        self.ser.close()

    def __del__(self):
        self.cleanup()
