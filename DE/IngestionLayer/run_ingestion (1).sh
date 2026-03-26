#!/bin/bash

# 1. Navigate to the project root directory
cd ~/de-prj/DE || exit

# 2. Activate your isolated virtual environment
source ~/de-venv/bin/activate

# 3. Run the utility script to ensure Kafka topics exist
echo "Setting up Kafka topics..."
python utility.py

# 4. Start the Real-Time Producer in the foreground for debugging
echo "---------------------------------------------------------"
echo "Starting Real-Time Producer..."
echo "Press Ctrl+C at any time to stop the stream."
echo "---------------------------------------------------------"

# We add PYTHONPATH="." to tell Python to include the current root directory in its search
PYTHONPATH="." python "IngestionLayer/real_time_producer.py"