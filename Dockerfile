# Use official Python image
FROM python:3.9.16

# Set the working directory
WORKDIR /src

# Install system dependencies (PostgreSQL client + Tor + curl for health checks)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    tor \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy only requirement.txt first to leverage Docker cache
COPY requirement.txt .

# Install dependencies
RUN pip install -r requirement.txt

# Copy application code and startup scripts
COPY . .

# Expose Flask app port
EXPOSE 5000

# Start Tor in the background & then run the app
CMD tor & sleep 5 && python main.py
