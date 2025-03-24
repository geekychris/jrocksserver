#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Setting up RocksDB Service Development Environment${NC}"

# Check Java version
if ! command -v java &> /dev/null; then
    echo -e "${RED}Java not found. Please install Java 17 or later.${NC}"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | sed '/^1\./s///' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt "17" ]; then
    echo -e "${RED}Java 17 or later is required. Found version: $JAVA_VERSION${NC}"
    exit 1
fi

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}Maven not found. Please install Maven 3.8 or later.${NC}"
    exit 1
fi

echo -e "\n${YELLOW}Building project...${NC}"
mvn clean package
if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed${NC}"
    exit 1
fi
echo -e "${GREEN}Build successful${NC}"

# Create Docker image if Docker is available
if command -v docker &> /dev/null; then
    echo -e "\n${YELLOW}Building Docker image...${NC}"
    cp target/rocksdb-service-0.0.1-SNAPSHOT.jar src/main/docker/
    cd src/main/docker
    docker build -t rocksdb-service .
    if [ $? -ne 0 ]; then
        echo -e "${RED}Docker build failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}Docker image built successfully${NC}"
    cd ../../..
else
    echo -e "\n${YELLOW}Docker not found. Skipping Docker image build.${NC}"
fi

# Create data directory
mkdir -p data

echo -e "\n${GREEN}Setup complete!${NC}"
echo -e "\n${YELLOW}Quick Start Guide:${NC}"
echo -e "1. Run the service locally:"
echo -e "   ${GREEN}java -jar target/rocksdb-service-0.0.1-SNAPSHOT.jar${NC}"
echo -e "\n2. Or run with Docker:"
echo -e "   ${GREEN}docker run -p 9090:9090 -v $(pwd)/data:/data rocksdb-service${NC}"
echo -e "\n3. Example API calls (using grpcurl):"
echo -e "   # Put a key-value pair:"
echo -e "   ${GREEN}grpcurl -plaintext -d '{\"database_name\":\"mydb\",\"key_values\":[{\"key\":\"a2V5\",\"value\":\"dmFsdWU=\"}]}' localhost:9090 com.example.rocksdb.RocksDBService/Put${NC}"
echo -e "\n   # Get a value:"
echo -e "   ${GREEN}grpcurl -plaintext -d '{\"database_name\":\"mydb\",\"keys\":[\"a2V5\"]}' localhost:9090 com.example.rocksdb.RocksDBService/Get${NC}"
echo -e "\n   # Get sequence number:"
echo -e "   ${GREEN}grpcurl -plaintext -d '{\"database_name\":\"mydb\"}' localhost:9090 com.example.rocksdb.RocksDBService/GetCurrentSequenceNumber${NC}"
echo -e "\nFor more examples, see README.md"

