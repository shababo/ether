from ether import ether_init
from ether.test_classes import MyService

# Initialize Ether functionality
ether_init()

# Verification
def verify_ether_class(cls):
    print(f"\nVerifying Class: {cls.__name__}:")
    print(f"Has Ether methods info: {hasattr(cls, '_ether_methods_info')}")
    if hasattr(cls, '_ether_methods_info'):
        print(f"Ether methods: {list(cls._ether_methods_info.keys())}")
    print(f"Has setup_sockets: {hasattr(cls, 'setup_sockets')}")
    print(f"Has receive_single_message: {hasattr(cls, 'receive_single_message')}")
    print(f"Has cleanup: {hasattr(cls, 'cleanup')}")

# Test
verify_ether_class(MyService)

# Create instance and verify functionality
service = MyService(name="test", value=42)
print(f"\nInstance verification:")
print(f"Has Ether methods: {hasattr(service, '_ether_methods_info')}")
print(f"Has receive_single_message: {hasattr(service, 'receive_single_message')}")