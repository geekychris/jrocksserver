package com.hitorro.rocksdbserver.service;

import com.hitorro.rocksdbserver.config.RocksDBConfiguration;
import com.hitorro.rocksdbserver.config.RocksDBProperties;
import com.hitorro.rocksdbserver.proto.*;
import com.google.protobuf.ByteString;
import com.hitorro.rocksdbserver.proto.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RocksDBGrpcServiceTest {
    private RocksDBGrpcService service;
    private RocksDBConfiguration configuration;
    private RocksDBProperties properties;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() throws Exception {
        properties = new RocksDBProperties();
        properties.setDataDir(tempDir.toString());
        properties.setCompressionEnabled(true);
        properties.setCompressionType("SNAPPY_COMPRESSION");
        properties.setBottommostCompressionType("ZSTD_COMPRESSION");
        configuration = new RocksDBConfiguration(properties);
        configuration.init();
        service = new RocksDBGrpcService(configuration);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        configuration.cleanup();
    }
    
    @Test
    void testBasicPutAndGet() throws Exception {
        String dbName = "testdb";
        String key = "testkey";
        String value = "testvalue";
        
        // Put
        PutRequest putRequest = PutRequest.newBuilder()
                .setDatabaseName(dbName)
                .addKeyValues(KeyValue.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8(value))
                        .build())
                .build();
        
        AtomicReference<PutResponse> putResponse = new AtomicReference<>();
        service.putSingle(putRequest, new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse response) {
                putResponse.set(response);
            }
            
            @Override
            public void onError(Throwable t) {
                fail("Put failed: " + t.getMessage());
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertNotNull(putResponse.get());
        assertEquals(Status.Code.OK,
                putResponse.get().getStatus().getCode());
        
        // Get
        GetRequest getRequest = GetRequest.newBuilder()
                .setDatabaseName(dbName)
                .addKeys(ByteString.copyFromUtf8(key))
                .build();
        
        AtomicReference<GetResponse> getResponse = new AtomicReference<>();
        service.get(getRequest, new StreamObserver<GetResponse>() {
            @Override
            public void onNext(GetResponse response) {
                getResponse.set(response);
            }
            
            @Override
            public void onError(Throwable t) {
                fail("Get failed: " + t.getMessage());
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertNotNull(getResponse.get());
        assertEquals(Status.Code.OK,
                getResponse.get().getStatus().getCode());
        assertEquals(1, getResponse.get().getKeyValuesCount());
        assertEquals(value, getResponse.get().getKeyValues(0)
                .getValue().toStringUtf8());
    }
    
    @Test
    void testGetByPrefix() throws Exception {
        String dbName = "testdb";
        String keyPrefix = "prefix";
        int numKeys = 5;
        
        // Put multiple keys with prefix
        for (int i = 0; i < numKeys; i++) {
            String key = keyPrefix + i;
            String value = "value" + i;
            
            PutRequest putRequest = PutRequest.newBuilder()
                    .setDatabaseName(dbName)
                    .addKeyValues(KeyValue.newBuilder()
                            .setKey(ByteString.copyFromUtf8(key))
                            .setValue(ByteString.copyFromUtf8(value))
                            .build())
                    .build();
            
            service.putSingle(putRequest, new StreamObserver<PutResponse>() {
                @Override
                public void onNext(PutResponse response) {}
                @Override
                public void onError(Throwable t) {
                    fail("Put failed: " + t.getMessage());
                }
                @Override
                public void onCompleted() {}
            });
        }
        
        // Get by prefix
        GetByPrefixRequest request = GetByPrefixRequest.newBuilder()
                .setDatabaseName(dbName)
                .setPrefix(ByteString.copyFromUtf8(keyPrefix))
                .build();
        
        AtomicReference<Integer> count = new AtomicReference<>(0);
        service.getByPrefix(request, new StreamObserver<KeyValue>() {
            @Override
            public void onNext(KeyValue keyValue) {
                count.updateAndGet(v -> v + 1);
                assertTrue(keyValue.getKey().toStringUtf8().startsWith(keyPrefix));
            }
            
            @Override
            public void onError(Throwable t) {
                fail("GetByPrefix failed: " + t.getMessage());
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertEquals(numKeys, count.get());
    }
    
    @Test
    void testGetCurrentSequenceNumber() throws Exception {
        String dbName = "testdb";
        String key = "testkey";
        String value = "testvalue";
        
        // Put a value to increment sequence
        PutRequest putRequest = PutRequest.newBuilder()
                .setDatabaseName(dbName)
                .addKeyValues(KeyValue.newBuilder()
                        .setKey(ByteString.copyFromUtf8(key))
                        .setValue(ByteString.copyFromUtf8(value))
                        .build())
                .build();
        
        service.putSingle(putRequest, new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse response) {}
            @Override
            public void onError(Throwable t) {
                fail("Put failed: " + t.getMessage());
            }
            @Override
            public void onCompleted() {}
        });
        
        // Get sequence number
        DatabaseRequest request = DatabaseRequest.newBuilder()
                .setDatabaseName(dbName)
                .build();
        
        AtomicReference<SequenceNumberResponse> response = new AtomicReference<>();
        service.getCurrentSequenceNumber(request, new StreamObserver<SequenceNumberResponse>() {
            @Override
            public void onNext(SequenceNumberResponse seq) {
                response.set(seq);
            }
            
            @Override
            public void onError(Throwable t) {
                fail("GetCurrentSequenceNumber failed: " + t.getMessage());
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertNotNull(response.get());
        assertEquals(Status.Code.OK,
                response.get().getStatus().getCode());
        assertTrue(response.get().getSequenceNumber() > 0);
    }

    @Test
    void testGetFromSequence() throws Exception {
        String dbName = "testdb";
        int numKeys = 5;
        
        // Put multiple values
        for (int i = 0; i < numKeys; i++) {
            String key = "key" + i;
            String value = "value" + i;
            
            PutRequest putRequest = PutRequest.newBuilder()
                    .setDatabaseName(dbName)
                    .addKeyValues(KeyValue.newBuilder()
                            .setKey(ByteString.copyFromUtf8(key))
                            .setValue(ByteString.copyFromUtf8(value))
                            .build())
                    .build();
            
            service.putSingle(putRequest, new StreamObserver<PutResponse>() {
                @Override
                public void onNext(PutResponse response) {}
                @Override
                public void onError(Throwable t) {
                    fail("Put failed: " + t.getMessage());
                }
                @Override
                public void onCompleted() {}
            });
        }
        
        // Get from sequence 0
        SequenceRequest request = SequenceRequest.newBuilder()
                .setDatabaseName(dbName)
                .setStartSequenceNumber(0)
                .setLimit(numKeys)
                .build();
        
        AtomicReference<Integer> count = new AtomicReference<>(0);
        service.getFromSequence(request, new StreamObserver<KeyValueWithSequence>() {
            @Override
            public void onNext(KeyValueWithSequence keyValue) {
                count.updateAndGet(v -> v + 1);
                assertTrue(keyValue.getSequenceNumber() > 0);
            }
            
            @Override
            public void onError(Throwable t) {
                fail("GetFromSequence failed: " + t.getMessage());
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertEquals(numKeys, count.get());
    }

    @Test
    void testDatabaseNotFound() throws Exception {
        String nonExistentDb = "nonexistentdb";
        
        // Try to get from non-existent database
        GetRequest request = GetRequest.newBuilder()
                .setDatabaseName(nonExistentDb)
                .addKeys(ByteString.copyFromUtf8("anykey"))
                .build();
        
        AtomicReference<GetResponse> response = new AtomicReference<>();
        service.get(request, new StreamObserver<GetResponse>() {
            @Override
            public void onNext(GetResponse getResponse) {
                response.set(getResponse);
            }
            
            @Override
            public void onError(Throwable t) {
                fail("Expected error response, not exception");
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertNotNull(response.get());
        assertEquals(Status.Code.DATABASE_NOT_FOUND,
                response.get().getStatus().getCode());
    }

    @Test
    void testPutEmptyKeyValue() throws Exception {
        String dbName = "testdb";
        
        // Try to put empty key
        PutRequest request = PutRequest.newBuilder()
                .setDatabaseName(dbName)
                .addKeyValues(KeyValue.newBuilder()
                        .setValue(ByteString.copyFromUtf8("value"))
                        .build())
                .build();
        
        AtomicReference<PutResponse> response = new AtomicReference<>();
        service.putSingle(request, new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse putResponse) {
                response.set(putResponse);
            }
            
            @Override
            public void onError(Throwable t) {
                fail("Expected error response, not exception");
            }
            
            @Override
            public void onCompleted() {}
        });
        
        assertNotNull(response.get());
        assertEquals(Status.Code.ERROR,
                response.get().getStatus().getCode());
        assertEquals(0, response.get().getSuccessCount());
    }
}

