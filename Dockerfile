FROM python:3.9.16

# Set the working directory
WORKDIR /src

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy and install Python dependencies
COPY requirement.txt .
RUN pip install -r requirement.txt

# Copy the application code
COPY . .

# Command to run the application
CMD ["python", "main.py"]
