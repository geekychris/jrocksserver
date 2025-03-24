package com.hitorro.rocksdbserver.service;

import com.hitorro.rocksdbserver.config.RocksDBConfiguration;
import com.hitorro.rocksdbserver.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class RocksDBGrpcServiceImpl extends RocksDBServiceGrpc.RocksDBServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBGrpcServiceImpl.class);
    private final RocksDBConfiguration rocksDBConfiguration;
    
    public RocksDBGrpcServiceImpl(RocksDBConfiguration rocksDBConfiguration) {
        this.rocksDBConfiguration = rocksDBConfiguration;
    }
    
    // Previous methods remain the same...
    // Add these new methods:
    
    @Override
    public StreamObserver<KeyValue> batchPut(StreamObserver<PutResponse> responseObserver) {
        return new StreamObserver<KeyValue>() {
            private int successCount = 0;
            private String currentDatabase = null;
            private RocksDB db = null;
            
            @Override
            public void onNext(KeyValue keyValue) {
                if (currentDatabase == null) {
                    currentDatabase = keyValue.getDatabaseName();
                    db = rocksDBConfiguration.getRocksDB(currentDatabase);
                    if (db == null) {
                        responseObserver.onNext(PutResponse.newBuilder()
                                .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                                        .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                                        .build())
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }
                }
                
                try {
                    db.put(rocksDBConfiguration.getWriteOptions(),
                            keyValue.getKey().toByteArray(),
                            keyValue.getValue().toByteArray());
                    successCount++;
                } catch (RocksDBException e) {
                    logger.error("Error in batch put operation", e);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                logger.error("Error in batch put stream", t);
                responseObserver.onNext(PutResponse.newBuilder()
                        .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.ERROR)
                                .setMessage(t.getMessage())
                                .build())
                        .setSuccessCount(successCount)
                        .build());
                responseObserver.onCompleted();
            }
            
            @Override
            public void onCompleted() {
                if (db != null) {
                    responseObserver.onNext(PutResponse.newBuilder()
                            .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                                    .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                                    .build())
                            .setSuccessCount(successCount)
                            .setNewSequenceNumber(db.getLatestSequenceNumber())
                            .build());
                }
                responseObserver.onCompleted();
            }
        };
    }
    
    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String databaseName = request.getDatabaseName();
        RocksDB db = rocksDBConfiguration.getRocksDB(databaseName);
        
        if (db == null) {
            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                            .build())
                    .build());
            responseObserver.onCompleted();
            return;
        }
        
        int successCount = 0;
        try {
            for (ByteString key : request.getKeysList()) {
                db.delete(rocksDBConfiguration.getWriteOptions(), key.toByteArray());
                successCount++;
            }
            
            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                            .build())
                    .setSuccessCount(successCount)
                    .setNewSequenceNumber(db.getLatestSequenceNumber())
                    .build());
        } catch (RocksDBException e) {
            logger.error("Error in delete operation", e);
            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.ERROR)
                            .setMessage(e.getMessage())
                            .build())
                    .setSuccessCount(successCount)
                    .build());
        }
        responseObserver.onCompleted();
    }
    
    @Override
    public void getCurrentSequenceNumber(DatabaseRequest request,
                                         StreamObserver<SequenceNumberResponse> responseObserver) {
        String databaseName = request.getDatabaseName();
        RocksDB db = rocksDBConfiguration.getRocksDB(databaseName);
        
        if (db == null) {
            responseObserver.onNext(SequenceNumberResponse.newBuilder()
                    .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                            .build())
                    .build());
        } else {
            responseObserver.onNext(SequenceNumberResponse.newBuilder()
                    .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                            .build())
                    .setSequenceNumber(db.getLatestSequenceNumber())
                    .build());
        }
        responseObserver.onCompleted();
    }
    
    @Override
    public void getFromSequence(SequenceRequest request,
            StreamObserver<KeyValueWithSequence> responseObserver) {
        String databaseName = request.getDatabaseName();
        RocksDB db = rocksDBConfiguration.getRocksDB(databaseName);
        
        if (db == null) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Database not found: " + databaseName)
                    .asRuntimeException());
            return;
        }
        
        try (RocksIterator iterator = db.newIterator(rocksDBConfiguration.getReadOptions())) {
            long currentSequence = request.getStartSequenceNumber();
            int count = 0;
            int limit = request.getLimit();
            
            for (iterator.seekToFirst(); iterator.isValid() && count < limit; iterator.next()) {
                responseObserver.onNext(KeyValueWithSequence.newBuilder()
                        .setKey(ByteString.copyFrom(iterator.key()))
                        .setValue(ByteString.copyFrom(iterator.value()))
                        .setSequenceNumber(currentSequence++)
                        .build());
                count++;
            }
        } catch (Exception e) {
            logger.error("Error in getFromSequence operation", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Error reading data: " + e.getMessage())
                    .asRuntimeException());
            return;
        }
        responseObserver.onCompleted();
    }
    
    @Override
    public void startReplication(ReplicationRequest request,
            StreamObserver<ReplicationResponse> responseObserver) {
        // TODO: Implement replication logic
        responseObserver.onNext(ReplicationResponse.newBuilder()
                .setStatus(com.hitorro.rocksdbserver.proto.Status.newBuilder()
                        .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                        .build())
                .build());
        responseObserver.onCompleted();
    }
    
    @Override
    public void streamReplication(ReplicationRequest request,
            StreamObserver<KeyValueWithSequence> responseObserver) {
        // TODO: Implement streaming replication logic
        responseObserver.onCompleted();
    }
}

