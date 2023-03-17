# Use the official Ubuntu image as the base image
FROM ubuntu:latest

# Install the necessary runtime dependencies for your application
RUN apt-get update && \
    apt-get install -y libglib2.0-0 libsm6 libxext6 libxrender-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy the built application from the host into the container
COPY dist/main /app/main

COPY config.yaml /app/config.yaml

# Set the entry point for the container to run the application
ENTRYPOINT ["/bin/bash", "-c", "/app/main"]
