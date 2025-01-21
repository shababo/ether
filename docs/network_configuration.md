# Ether Network Configuration Guide

## Overview
Ether uses ZeroMQ (ZMQ) for network communication and Redis for state management. This guide explains how to configure networking for both local and remote connections.

## Port Requirements
Ether requires the following TCP ports for communication. Default ports are:
- 5555: PubSub Frontend
- 5556: PubSub Backend
- 5559: ReqRep Frontend
- 5560: ReqRep Backend
- 6379: Redis
- 301309: Session Discovery
- 301310: Session Query

> **Note:** All ports can be customized, but must match between server and clients.

For example, you could use different ports:
```python
network_config = EtherNetworkConfig(
    host="your.server.ip",
    pubsub_frontend_port=6555,  # Custom port
    pubsub_backend_port=6556,   # Custom port
    reqrep_frontend_port=6559,  # Custom port
    reqrep_backend_port=6560,   # Custom port
    redis_port=6380             # Custom Redis port
)
```

Just ensure that both server and clients use the same configuration:
```python
# Server and all clients must use matching configuration
server_config = EtherConfig(network=network_config)
client_config = EtherConfig(network=network_config)

## Local Development
For local development, no special configuration is needed. Ether will automatically use localhost (127.0.0.1) for all connections.

## Remote Connections
To allow remote connections to an Ether server:

1. **IP Configuration**
   - Reserve a static IP for the server machine
   - Note the server's public IP address

2. **Router Configuration**
   - Configure port forwarding for all required ports:
     ```
     TCP 5555 -> Server Local IP  (PubSub Frontend)
     TCP 5556 -> Server Local IP  (PubSub Backend)
     TCP 5559 -> Server Local IP  (ReqRep Frontend)
     TCP 5560 -> Server Local IP  (ReqRep Backend)
     TCP 6379 -> Server Local IP  (Redis)
     ```

3. **Server Configuration**
   ```python
   from ether import ether
   from ether._internal._config import EtherConfig, EtherNetworkConfig

   # Configure server with public IP
   network_config = EtherNetworkConfig(
       host="your.public.ip",  # Server's public IP
       pubsub_frontend_port=5555,
       pubsub_backend_port=5556,
       reqrep_frontend_port=5559,
       reqrep_backend_port=5560,
       redis_port=6379
   )

   config = EtherConfig(network=network_config)
   ether.tap(config=config)
   ```

4. **Client Configuration**
   ```python
   # Configure client to connect to server
   network_config = EtherNetworkConfig(
       host="server.public.ip",  # Server's public IP
       # ... same port configuration as server
   )
   ```

## Testing Connectivity

1. **Basic Port Test**
   ```bash
   # On server
   nc -l 5555

   # On client
   nc -v server.public.ip 5555
   ```

2. **Network Diagnostics**
   ```bash
   # Run the diagnostic script
   python src/tests/network_setup/diagnose_connection.py server.public.ip
   ```

## Troubleshooting

1. **Connection Refused**
   - Verify port forwarding configuration
   - Check server is running
   - Ensure no firewall blocking

2. **Connection Timeout**
   - Check router port forwarding
   - Verify server's public IP
   - Test basic connectivity (ping, traceroute)

3. **Server Not Found**
   - Verify DNS resolution
   - Check network configuration
   - Ensure correct IP address

## Security Considerations

1. **Port Exposure**
   - Only forward required ports
   - Use firewall rules to limit access

2. **Redis Security**
   - Configure Redis password if needed
   - Limit Redis access to trusted IPs

3. **Network Isolation**
   - Consider VPN for sensitive deployments
   - Use network segmentation when possible

## Example Configurations

1. **Local Development**
   ```python
   # Default configuration uses localhost
   ether.tap()
   ```

2. **Remote Server**
   ```python
   config = EtherConfig(
       network=EtherNetworkConfig(
           host="192.168.1.100",  # Local network
           # ... port configuration
       )
   )
   ```

3. **Public Server**
   ```python
   config = EtherConfig(
       network=EtherNetworkConfig(
           host="203.0.113.1",  # Public IP
           # ... port configuration
       )
   )
   ``` 