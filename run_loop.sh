#!/bin/sh
while true; do
    echo "Running main.py..."
    python /src/main.py  # Adjust the path if needed
    echo "Sleeping for 3 hours..."
    sleep 10800  # 10800 seconds = 3 hours
done
