package com.hitorro.rocksdbserver.config;

import com.hitorro.rocksdbserver.service.RocksDBGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;

@Component
public class GrpcServerConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServerConfiguration.class);

    private final RocksDBProperties properties;
    private final RocksDBGrpcService rocksDBService;
    private Server server;

    public GrpcServerConfiguration(RocksDBProperties properties, RocksDBGrpcService rocksDBService) {
        this.properties = properties;
        this.rocksDBService = rocksDBService;
    }

    @PostConstruct
    public void start() throws IOException {
        int port = properties.getGrpc().getPort();
        server = ServerBuilder.forPort(port)
                .addService(rocksDBService)
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();
        
        logger.info("gRPC Server started on port {}", port);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down gRPC server due to JVM shutdown");
            GrpcServerConfiguration.this.stop();
        }));
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            logger.info("Shutting down gRPC server");
            server.shutdown();
            try {
                // Wait for server to terminate
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("gRPC server shutdown interrupted", e);
            }
        }
    }
}

