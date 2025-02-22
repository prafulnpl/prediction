# Use official Python image
FROM python:3.9.16

# Set the working directory
WORKDIR /src

# Install system dependencies (PostgreSQL client + Tor)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    tor \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy and install Python dependencies
COPY requirement.txt .
RUN pip install -r requirement.txt

# Copy the application code
COPY . .

# Expose Flask app port
EXPOSE 5000

# Run Tor in the background before starting the application
CMD tor & /src/run_loop.sh 

