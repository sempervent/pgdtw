# Use the official PostgreSQL image as a base
FROM postgres:latest

# Set environment variables for PostgreSQL
ENV POSTGRES_DB=yourdbname
ENV POSTGRES_USER=yourusername
ENV POSTGRES_PASSWORD=yourpassword

# Copy your extension files into the Docker image
COPY pg_dtw.c pg_dtw--0.1.sql Makefile /pg_dtw/

# Install necessary packages
RUN apt-get update && \
    apt-get install -y gcc postgresql-server-dev-all make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Compile and install the extension
RUN cd /pg_dtw && \
    make && \
    make install

# Automate the creation of the extension in the database
RUN echo "CREATE EXTENSION pg_dtw;" > /docker-entrypoint-initdb.d/init-user-db.sh

# The base image already sets the default CMD to start PostgreSQL
