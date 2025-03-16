# Use the official Python image
FROM python:3.9.16

# Set the working directory in the container
WORKDIR /src

# Install system dependencies (PostgreSQL client and curl for health checks)
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy only requirement.txt first to leverage Docker cache
COPY requirement.txt .

# Install Python dependencies from requirement.txt
RUN pip install -r requirement.txt

# Copy the rest of the application code (this layer will be overridden by the volume mount during development)
COPY . .

# Expose the application port (e.g., Flask app running on port 5000)
EXPOSE 5000

# Start the app
CMD python main.py
