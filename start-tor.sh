#!/bin/bash
# Start Tor in the background
tor &

# Wait for Tor to become available
while ! curl --socks5-hostname localhost:9050 -s https://check.torproject.org/ > /dev/null; do
    echo "Waiting for Tor connection..."
    sleep 1
done

echo "Tor is running. Starting application..."
exec "$@"