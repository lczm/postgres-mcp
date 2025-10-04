FROM postgres:16-alpine

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

# Set default environment variables
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=testdb
