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
    ufw allow 13311/tcp  # PubSub frontend
    ufw allow 13312/tcp  # PubSub backend
    ufw allow 13313/tcp  # ReqRep frontend
    ufw allow 13314/tcp  # ReqRep backend
    ufw allow 13315/tcp  # Redis
    
    echo "UFW rules added. Current status:"
    ufw status | grep -E '13311|13312|13313|13314|13315'
    
elif command -v iptables >/dev/null 2>&1; then
    echo "Using iptables..."
    
    # Allow ZMQ and Redis ports
    iptables -A INPUT -p tcp --dport 13311 -j ACCEPT
    iptables -A INPUT -p tcp --dport 13312 -j ACCEPT
    iptables -A INPUT -p tcp --dport 13313 -j ACCEPT
    iptables -A INPUT -p tcp --dport 13314 -j ACCEPT
    iptables -A INPUT -p tcp --dport 13315 -j ACCEPT
    
    echo "IPTables rules added. Current rules:"
    iptables -L | grep -E '13311|13312|13313|13314|13315'
else
    echo "No supported firewall found"
    exit 1
fi

echo "Ports opened successfully" 