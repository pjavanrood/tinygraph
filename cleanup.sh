#!/bin/bash

# Cleanup script for TinyGraph cluster
# Usage: ./cleanup.sh <config_file>
#
# This script reads a TinyGraph config file and kills all processes
# running on ports defined in the configuration.

set -e

# Check if config file is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <config_file>"
    echo "Example: $0 config.yaml"
    exit 1
fi

CONFIG_FILE="$1"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file '$CONFIG_FILE' not found"
    exit 1
fi

echo "Starting cleanup for config: $CONFIG_FILE"
echo "=========================================="

# Extract all ports from the config file
# This includes query_manager port, rpc_port, and raft_port
PORTS=$(grep -E '(port:|rpc_port:|raft_port:)' "$CONFIG_FILE" | \
        grep -v '#' | \
        awk '{print $2}' | \
        sort -u)

if [ -z "$PORTS" ]; then
    echo "No ports found in config file"
    exit 0
fi

echo "Found ports: $(echo $PORTS | tr '\n' ' ')"
echo ""

# Function to kill process on a given port
kill_process_on_port() {
    local port=$1
    echo "Checking port $port..."
    
    # Find PIDs using lsof
    PIDS=$(lsof -ti :$port 2>/dev/null || true)
    
    if [ -z "$PIDS" ]; then
        echo "  No process found on port $port"
        return
    fi
    
    # Kill each process
    for pid in $PIDS; do
        # Get process info before killing
        PROCESS_INFO=$(ps -p $pid -o comm= 2>/dev/null || echo "unknown")
        echo "  Found process: $PROCESS_INFO (PID: $pid)"
        
        # Try graceful kill first
        if kill $pid 2>/dev/null; then
            echo "  Sent SIGTERM to PID $pid"
            
            # Wait a bit for graceful shutdown
            sleep 1
            
            # Check if process still exists
            if ps -p $pid > /dev/null 2>&1; then
                echo "  Process still running, sending SIGKILL..."
                kill -9 $pid 2>/dev/null || true
            fi
            
            echo "  ✓ Killed process on port $port (PID: $pid)"
        else
            echo "  ✗ Failed to kill PID $pid (may require sudo)"
        fi
    done
}

# Kill processes on all found ports
for port in $PORTS; do
    kill_process_on_port $port
done

echo ""
echo "Removing logs..."
if [ -d "logs" ]; then
    rm -rf logs/*
    echo "  ✓ Removed logs directory"
else
    echo "  No logs directory found"
fi

echo ""
echo "=========================================="
echo "Cleanup complete!"

