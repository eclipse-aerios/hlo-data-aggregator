# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set an environment variable to ensure Python outputs are sent straight to terminal without being first buffered
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /src

# Create a virtual environment to isolate our package dependencies locally
RUN python -m venv venv
ENV PATH="/src/venv/bin:$PATH"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /src
COPY requirements.txt src/ /src/

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Run app.py when the container launches
ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
