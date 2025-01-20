import socket
import sys
import subprocess
import platform
import os
import requests

def get_network_info():
    """Get basic network information"""
    info = {}
    
    # Get all IP addresses
    hostname = socket.gethostname()
    try:
        info['hostname'] = hostname
        info['local_ip'] = socket.gethostbyname(hostname)
        
        # Try to get public IP
        try:
            response = requests.get('https://api.ipify.org', timeout=2)
            info['public_ip'] = response.text.strip()
        except:
            info['public_ip'] = "Could not determine"
            
    except Exception as e:
        print(f"Error getting network info: {e}")
    
    return info

def test_local_ports(host):
    """Test if ports are in use locally"""
    ports = [5555, 5556, 5559, 5560, 6379]
    results = {}
    
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # Try to bind to the port
            sock.bind((host, port))
            results[port] = "Available (not in use)"
        except socket.error as e:
            results[port] = f"In use or blocked ({str(e)})"
        finally:
            sock.close()
    
    return results

def run_command(command):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Error running command: {e}"

def main():
    if len(sys.argv) != 2:
        print("Usage: python diagnose_connection.py <target_host>")
        sys.exit(1)
    
    target_host = sys.argv[1]
    
    print("\n=== Network Diagnostics ===")
    print("\n1. Basic Network Information:")
    info = get_network_info()
    for key, value in info.items():
        print(f"{key}: {value}")
    
    print("\n2. Local Port Status:")
    port_status = test_local_ports('0.0.0.0')
    for port, status in port_status.items():
        print(f"Port {port}: {status}")
    
    print("\n3. Connection Test to Target:")
    ports = [5555, 5556, 5559, 5560, 6379]
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        try:
            result = sock.connect_ex((target_host, port))
            print(f"Port {port}: {'Success' if result == 0 else f'Failed (error code: {result})'}")
        except socket.error as e:
            print(f"Port {port}: Error - {e}")
        finally:
            sock.close()
    
    print("\n4. Firewall Status:")
    if platform.system() == "Linux":
        print("\nUFW Status:")
        print(run_command("sudo ufw status"))
        print("\nIPTables Rules:")
        print(run_command("sudo iptables -L"))
    elif platform.system() == "Darwin":  # macOS
        print("\nPF Status:")
        print(run_command("sudo pfctl -s all"))
    
    print("\n5. Route to Target:")
    if platform.system() in ["Linux", "Darwin"]:
        print(run_command(f"traceroute -n {target_host}"))
    
    print("\n6. Redis Status:")
    print(run_command("ps aux | grep redis-server"))
    
    print("\n7. Active Network Connections:")
    if platform.system() in ["Linux", "Darwin"]:
        print(run_command("netstat -an | grep -E '5555|5556|5559|5560|6379'"))

if __name__ == "__main__":
    main() 