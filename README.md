# RocksDB Service

A gRPC service providing multi-database RocksDB access with streaming and replication capabilities.

## Features

- Multiple RocksDB database support
- CRUD operations with streaming capability
- Prefix-based key retrieval
- Sequence-based operations
- Cross-instance replication

## Building

### Prerequisites

- Java 17 or later
- Maven 3.8 or later
- Docker (optional, for containerization)

### Build Commands

Build the service:
```bash
mvn clean package
```

Build Docker image:
```bash
# Copy jar to docker directory
cp target/rocksdb-service-0.0.1-SNAPSHOT.jar src/main/docker/

# Build image
cd src/main/docker
docker build -t rocksdb-service .
```

```plantuml
A -> B: abc
```

## Running

### Local
```bash
java -jar target/rocksdb-service-0.0.1-SNAPSHOT.jar
```

### Docker
```bash
docker run -p 9090:9090 -v /path/to/data:/data rocksdb-service
```

## Configuration

Configuration is done through `application.yml`. Key settings:

```yaml
rocksdb:
  data-dir: ./data      # Database files location
  grpc:
    port: 9090          # gRPC server port
```

## API Usage Examples

Using [grpcurl](https://github.com/fullstorydev/grpcurl) for command-line testing:

### Basic Operations

Put values:
```bash
grpcurl -plaintext -d '{
  "database_name": "mydb",
  "key_values": [
    {
      "key": "a2V5MQ==",
      "value": "dmFsdWUx" 
    }
  ]
}' localhost:9095 com.example.rocksdb.RocksDBService/Put
```

Get values:
```bash
grpcurl -plaintext -d '{
  "database_name": "mydb",
  "keys": ["a2V5MQ=="]
}' localhost:9095 com.example.rocksdb.RocksDBService/Get
```

Get by prefix:
```bash
grpcurl -plaintext -d '{
  "database_name": "mydb",
  "prefix": "a2V5", 
  "limit": 10
}' localhost:9090 com.example.rocksdb.RocksDBService/GetByPrefix
```

### Sequence Operations

Get current sequence number:
```bash
grpcurl -plaintext -d '{
  "database_name": "mydb"
}' localhost:9095 com.example.rocksdb.RocksDBService/GetCurrentSequenceNumber
```

Get from sequence:
```bash
grpcurl -plaintext -d '{
  "database_name": "mydb",
  "start_sequence_number": 100,
  "limit": 1000
}' localhost:9095 com.example.rocksdb.RocksDBService/GetFromSequence
```

### Replication

Start replication:
```bash
grpcurl -plaintext -d '{
  "source_host": "source.example.com",
  "source_port": 9090,
  "source_database_name": "sourcedb",
  "target_database_name": "targetdb",
  "start_sequence_number": 0,
  "stream_mode": true
}' localhost:9095 com.example.rocksdb.RocksDBService/StartReplication
```

## Monitoring & Operations

### Logging

The service uses SLF4J with logback for logging. Configure levels in `application.yml`:

```yaml
logging:
  level:
    com.example.rocksdb: DEBUG
```

### Health Check

The service is healthy if it responds to gRPC requests. Example health check:

```bash
grpcurl -plaintext -d '{"database_name": "mydb"}' \
  localhost:9095 com.example.rocksdb.RocksDBService/GetCurrentSequenceNumber
```

## Development

### Adding New Features

1. Modify `src/main/proto/rocksdb_service.proto`
2. Rebuild to generate new gRPC code
3. Implement new methods in `RocksDBGrpcService.java`

### Testing

Run tests:
```bash
mvn test
```

