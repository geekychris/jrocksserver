FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Add startup script
COPY start.sh /app/
RUN chmod +x /app/start.sh

# Copy the built jar file
COPY *.jar /app/service.jar

# Set environment variables
ENV JAVA_OPTS=""
ENV ROCKSDB_DATA_DIR="/data"

# Create data directory
RUN mkdir -p /data

# Expose gRPC port
EXPOSE 9090

# Start the service
CMD ["/app/start.sh"]

