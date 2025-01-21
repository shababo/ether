#!/bin/bash

# Check if script is run as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Detect firewall type
if command -v ufw >/dev/null 2>&1; then
    echo "Using UFW firewall..."
    
    # Allow ZMQ and Redis ports
    ufw allow 5555/tcp  # PubSub frontend
    ufw allow 5556/tcp  # PubSub backend
    ufw allow 5559/tcp  # ReqRep frontend
    ufw allow 5560/tcp  # ReqRep backend
    ufw allow 6379/tcp  # Redis
    
    echo "UFW rules added. Current status:"
    ufw status | grep -E '5555|5556|5559|5560|6379'
    
elif command -v iptables >/dev/null 2>&1; then
    echo "Using iptables..."
    
    # Allow ZMQ and Redis ports
    iptables -A INPUT -p tcp --dport 5555 -j ACCEPT
    iptables -A INPUT -p tcp --dport 5556 -j ACCEPT
    iptables -A INPUT -p tcp --dport 5559 -j ACCEPT
    iptables -A INPUT -p tcp --dport 5560 -j ACCEPT
    iptables -A INPUT -p tcp --dport 6379 -j ACCEPT
    
    echo "IPTables rules added. Current rules:"
    iptables -L | grep -E '5555|5556|5559|5560|6379'
else
    echo "No supported firewall found"
    exit 1
fi

echo "Ports opened successfully" 