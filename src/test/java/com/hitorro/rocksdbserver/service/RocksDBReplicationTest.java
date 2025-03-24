package com.hitorro.rocksdbserver.service;

import com.hitorro.rocksdbserver.config.RocksDBConfiguration;
import com.hitorro.rocksdbserver.config.RocksDBProperties;
import com.hitorro.rocksdbserver.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RocksDBReplicationTest {
    
    private RocksDBGrpcService sourceService;
    private RocksDBGrpcService targetService;
    private RocksDBConfiguration sourceConfig;
    private RocksDBConfiguration targetConfig;
    
    @TempDir
    Path sourceTempDir;
    
    @TempDir
    Path targetTempDir;
    
    @BeforeEach
    void setUp() throws Exception {
        // Set up source service
        RocksDBProperties sourceProps = new RocksDBProperties();
        sourceProps.setDataDir(sourceTempDir.toString());
        sourceProps.setCompressionEnabled(true);
        sourceProps.setCompressionType("SNAPPY_COMPRESSION");
        sourceProps.setBottommostCompressionType("ZSTD_COMPRESSION");
        sourceConfig = new RocksDBConfiguration(sourceProps);
        sourceConfig.init();
        sourceService = new RocksDBGrpcService(sourceConfig);
        
        // Set up target service
        RocksDBProperties targetProps = new RocksDBProperties();
        targetProps.setDataDir(targetTempDir.toString());
        targetProps.setCompressionEnabled(true);
        targetProps.setCompressionType("SNAPPY_COMPRESSION");
        targetProps.setBottommostCompressionType("ZSTD_COMPRESSION");
        targetConfig = new RocksDBConfiguration(targetProps);
        targetConfig.init();
        targetService = new RocksDBGrpcService(targetConfig);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        sourceConfig.cleanup();
        targetConfig.cleanup();
    }
    
    @Test
    void testStreamReplication() throws InterruptedException {
        String sourceDb = "sourcedb";
        String targetDb = "targetdb";
        int numKeys = 10;
        
        // Put data in source database
        for (int i = 0; i < numKeys; i++) {
            PutRequest putRequest = PutRequest.newBuilder()
                    .setDatabaseName(sourceDb)
                    .addKeyValues(KeyValue.newBuilder()
                            .setKey(ByteString.copyFromUtf8("key" + i))
                            .setValue(ByteString.copyFromUtf8("value" + i))
                            .build())
                    .build();
            
            CountDownLatch putLatch = new CountDownLatch(1);
            AtomicBoolean putSuccess = new AtomicBoolean(false);
            
            sourceService.putSingle(putRequest, new StreamObserver<PutResponse>() {
                @Override
                public void onNext(PutResponse response) {
                    putSuccess.set(response.getStatus().getCode() == 
                            Status.Code.OK);
                }
                
                @Override
                public void onError(Throwable t) {
                    putSuccess.set(false);
                    putLatch.countDown();
                }
                
                @Override
                public void onCompleted() {
                    putLatch.countDown();
                }
            });
            
            assertTrue(putLatch.await(5, TimeUnit.SECONDS));
            assertTrue(putSuccess.get());
        }
        
        // Start replication
        ReplicationRequest request = ReplicationRequest.newBuilder()
                .setSourceHost("localhost")
                .setSourcePort(9090)
                .setSourceDatabaseName(sourceDb)
                .setTargetDatabaseName(targetDb)
                .setStartSequenceNumber(0)
                .setStreamMode(true)
                .build();
        
        CountDownLatch replicationLatch = new CountDownLatch(1);
        List<KeyValueWithSequence> replicatedData = new ArrayList<>();
        AtomicBoolean replicationSuccess = new AtomicBoolean(true);
        
        targetService.streamReplication(request, new StreamObserver<KeyValueWithSequence>() {
            @Override
            public void onNext(KeyValueWithSequence value) {
                replicatedData.add(value);
            }
            
            @Override
            public void onError(Throwable t) {
                replicationSuccess.set(false);
                replicationLatch.countDown();
            }
            
            @Override
            public void onCompleted() {
                replicationLatch.countDown();
            }
        });
        
        assertTrue(replicationLatch.await(10, TimeUnit.SECONDS));
        assertTrue(replicationSuccess.get());
        assertEquals(numKeys, replicatedData.size());
        
        // Verify replicated data
        for (int i = 0; i < numKeys; i++) {
            String expectedKey = "key" + i;
            String expectedValue = "value" + i;
            
            boolean found = replicatedData.stream()
                    .anyMatch(kv -> 
                            kv.getKey().toStringUtf8().equals(expectedKey) &&
                            kv.getValue().toStringUtf8().equals(expectedValue));
            assertTrue(found, "Key " + expectedKey + " not found in replicated data");
        }
    }
    
    @Test
    void testReplicationWithInvalidSource() throws Exception {
        ReplicationRequest request = ReplicationRequest.newBuilder()
                .setSourceHost("invalid-host")
                .setSourcePort(9090)
                .setSourceDatabaseName("sourcedb")
                .setTargetDatabaseName("targetdb")
                .setStartSequenceNumber(0)
                .build();
        
        AtomicReference<ReplicationResponse> response = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        
        targetService.startReplication(request, new StreamObserver<ReplicationResponse>() {
            @Override
            public void onNext(ReplicationResponse value) {
                response.set(value);
            }
            
            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }
            
            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        
        assertNotNull(response.get());
        assertEquals(Status.Code.REPLICATION_ERROR,
                response.get().getStatus().getCode());
    }
}

