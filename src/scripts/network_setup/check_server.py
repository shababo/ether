import socket
import sys
import time

def check_port(host: str, port: int, timeout: float = 1.0) -> bool:
    """Test if a port is open
    
    Args:
        host: Host to check
        port: Port number to check
        timeout: How long to wait for connection
        
    Returns:
        True if port is open, False otherwise
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    
    try:
        result = sock.connect_ex((host, port))
        return result == 0
    except socket.error as e:
        print(f"Error checking {host}:{port} - {e}")
        return False
    finally:
        sock.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python check_server.py <host>")
        sys.exit(1)
        
    host = sys.argv[1]
    ports = {
        5555: "PubSub Frontend",
        5556: "PubSub Backend",
        5559: "ReqRep Frontend",
        5560: "ReqRep Backend",
        6379: "Redis"
    }
    
    print(f"\nChecking Ether ports on {host}...")
    print("-" * 50)
    
    any_open = False
    for port, name in ports.items():
        is_open = check_port(host, port)
        status = "OPEN" if is_open else "CLOSED"
        print(f"Port {port} ({name}): {status}")
        any_open = any_open or is_open
    
    print("-" * 50)
    if not any_open:
        print("\nNo ports are open. Make sure:")
        print("1. The Ether server is running")
        print("2. Firewall ports are open (run the open_ports script)")
        print("3. You're using the correct host address")
    else:
        print("\nSome ports are open and responding!")

if __name__ == "__main__":
    main() 