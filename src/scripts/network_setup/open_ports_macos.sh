#!/bin/bash

# Check if script is run as root
if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (use sudo)"
    exit 1
fi

# Get Python path
PYTHON_PATH=$(which python3)
if [ -z "$PYTHON_PATH" ]; then
    echo "Python3 not found"
    exit 1
fi

echo "Configuring firewall for Python at: $PYTHON_PATH"

# Configure application firewall for Python
/usr/libexec/ApplicationFirewall/socketfilter --add "$PYTHON_PATH"
/usr/libexec/ApplicationFirewall/socketfilter --unblock "$PYTHON_PATH"

echo "Firewall configured for Python. The following ports should now be accessible:"
echo "- 13311 (PubSub frontend)"
echo "- 13312 (PubSub backend)"
echo "- 13313 (ReqRep frontend)"
echo "- 13314 (ReqRep backend)"
echo "- 13315 (Redis)"

# Restart firewall to apply changes
pfctl -F all -f /etc/pf.conf 2>/dev/null

echo "Firewall rules applied successfully" 