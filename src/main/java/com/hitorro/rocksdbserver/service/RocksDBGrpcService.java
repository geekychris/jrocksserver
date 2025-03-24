package com.hitorro.rocksdbserver.service;

import com.hitorro.rocksdbserver.config.RocksDBConfiguration;
import com.hitorro.rocksdbserver.proto.*;
import com.google.protobuf.ByteString;
import com.hitorro.rocksdbserver.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;

// hello
@Service
public class RocksDBGrpcService extends RocksDBServiceGrpc.RocksDBServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBGrpcService.class);

    private final RocksDBConfiguration rocksDBConfiguration;
    private final Map<String, ReplicationSession> replicationSessions = new ConcurrentHashMap<>();
    
    // Timeout for replication client connections in milliseconds
    private static final int REPLICATION_CLIENT_TIMEOUT_MS = 30000;

    public RocksDBGrpcService(RocksDBConfiguration rocksDBConfiguration) {
        this.rocksDBConfiguration = rocksDBConfiguration;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String databaseName = request.getDatabaseName();
        logger.info("Processing get request for database: {}", databaseName);

        try {
            RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
            if (rocksDB == null) {
                com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                    .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                    .setMessage("Database not found: " + databaseName)
                    .build();
                GetResponse response = GetResponse.newBuilder()
                    .setStatus(status)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            List<KeyValue> keyValues = new ArrayList<>();
            for (ByteString key : request.getKeysList()) {
                byte[] value = rocksDB.get(key.toByteArray());
                if (value != null) {
                    KeyValue keyValue = KeyValue.newBuilder()
                        .setKey(key)
                        .setValue(ByteString.copyFrom(value))
                        .setDatabaseName(databaseName)
                        .build();
                    keyValues.add(keyValue);
                }
            }

            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                .build();

            GetResponse response = GetResponse.newBuilder()
                .addAllKeyValues(keyValues)
                .setStatus(status)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.debug("Get request completed successfully for database: {}", databaseName);
        } catch (RocksDBException e) {
            logger.error("Error getting values from database: {}", databaseName, e);
            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.ERROR)
                .setMessage("RocksDB error: " + e.getMessage())
                .build();
            GetResponse response = GetResponse.newBuilder()
                .setStatus(status)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getByPrefix(GetByPrefixRequest request, StreamObserver<KeyValue> responseObserver) {
        String databaseName = request.getDatabaseName();
        byte[] prefix = request.getPrefix().toByteArray();
        int limit = request.getLimit();

        logger.info("Processing getByPrefix request for database: {}", databaseName);

        try {
            RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
            if (rocksDB == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Database not found: " + databaseName)
                        .asRuntimeException());
                return;
            }

            int count = 0;
            try (RocksIterator iterator = rocksDB.newIterator()) {
                iterator.seek(prefix);
                while (iterator.isValid() && (limit == 0 || count < limit)) {
                    byte[] key = iterator.key();
                    if (!startsWith(key, prefix)) {
                        break;
                    }

                    KeyValue keyValue = KeyValue.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(iterator.value()))
                        .setDatabaseName(databaseName)
                        .build();
                    responseObserver.onNext(keyValue);

                    iterator.next();
                    count++;
                }
            }

            responseObserver.onCompleted();
            logger.debug("GetByPrefix completed successfully for database: {}, returned {} keys",
                    databaseName, count);
        } catch (Exception e) {
            logger.error("Error in getByPrefix for database: {}", databaseName, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    private boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void putSingle(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String databaseName = request.getDatabaseName();
        logger.info("Processing put request for database: {}", databaseName);

        try {
            RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
            if (rocksDB == null) {
                com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                    .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                    .setMessage("Database not found: " + databaseName)
                    .build();
                PutResponse response = PutResponse.newBuilder()
                    .setStatus(status)
                    .setSuccessCount(0)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            int keyCount = 0;
            try (WriteBatch batch = new WriteBatch()) {
                for (KeyValue kv : request.getKeyValuesList()) {
                    batch.put(kv.getKey().toByteArray(), kv.getValue().toByteArray());
                    keyCount++;
                }

                rocksDB.write(rocksDBConfiguration.getWriteOptions(), batch);
            }

            long newSequenceNumber = rocksDB.getLatestSequenceNumber();

            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                .build();

            PutResponse response = PutResponse.newBuilder()
                .setStatus(status)
                .setSuccessCount(keyCount)
                .setNewSequenceNumber(newSequenceNumber)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.debug("Put request completed successfully for database: {}, wrote {} keys",
                    databaseName, keyCount);
        } catch (RocksDBException e) {
            logger.error("Error writing to database: {}", databaseName, e);
            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.ERROR)
                .setMessage("RocksDB error: " + e.getMessage())
                .build();
            PutResponse response = PutResponse.newBuilder()
                .setStatus(status)
                .setSuccessCount(0)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<PutRequest> put(StreamObserver<PutResponse> responseObserver) {
        return new StreamObserver<PutRequest>() {
            private String databaseName;
            private WriteBatch batch = new WriteBatch();
            private int keyCount = 0;
            private RocksDB rocksDB;

            @Override
            public void onNext(PutRequest request) {
                try {
                    if (databaseName == null) {
                        databaseName = request.getDatabaseName();
                        rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
                        if (rocksDB == null) {
                            throw new IllegalStateException("Database not found: " + databaseName);
                        }
                    }

                    for (KeyValue kv : request.getKeyValuesList()) {
                        batch.put(kv.getKey().toByteArray(), kv.getValue().toByteArray());
                        keyCount++;
                    }
                } catch (Exception e) {
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Error in streaming put operation", t);
                try {
                    if (batch != null) {
                        batch.close();
                    }
                } catch (Exception e) {
                    logger.error("Error closing batch", e);
                }

                com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                    .setCode(com.hitorro.rocksdbserver.proto.Status.Code.ERROR)
                    .setMessage(t.getMessage())
                    .build();
                    
                responseObserver.onNext(PutResponse.newBuilder()
                    .setStatus(status)
                    .setSuccessCount(0)
                    .build());
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                try {
                    if (rocksDB == null) {
                        throw new IllegalStateException("No database specified in the request stream");
                    }

                    rocksDB.write(rocksDBConfiguration.getWriteOptions(), batch);
                    long newSequenceNumber = rocksDB.getLatestSequenceNumber();

                    com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                        .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                        .build();

                    responseObserver.onNext(PutResponse.newBuilder()
                        .setStatus(status)
                        .setSuccessCount(keyCount)
                        .setNewSequenceNumber(newSequenceNumber)
                        .build());
                    responseObserver.onCompleted();
                    
                    logger.debug("Streaming put completed successfully for database: {}, wrote {} keys",
                        databaseName, keyCount);
                } catch (Exception e) {
                    onError(e);
                } finally {
                    try {
                        if (batch != null) {
                            batch.close();
                        }
                    } catch (Exception e) {
                        logger.error("Error closing batch", e);
                    }
                }
            }
        };
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String databaseName = request.getDatabaseName();
        logger.info("Processing delete request for database: {}", databaseName);

        RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
        if (rocksDB == null) {
            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                .setMessage("Database not found: " + databaseName)
                .build();
            DeleteResponse response = DeleteResponse.newBuilder()
                .setStatus(status)
                .setSuccessCount(0)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        int successCount = 0;
        WriteOptions writeOptions = rocksDBConfiguration.getWriteOptions();

        for (ByteString key : request.getKeysList()) {
            try {
                rocksDB.delete(writeOptions, key.toByteArray());
                successCount++;
            } catch (RocksDBException e) {
                logger.warn("Failed to delete key from database: {}", databaseName, e);
            }
        }

        long newSequenceNumber = rocksDB.getLatestSequenceNumber();

        com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
            .build();

        DeleteResponse response = DeleteResponse.newBuilder()
            .setStatus(status)
            .setSuccessCount(successCount)
            .setNewSequenceNumber(newSequenceNumber)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        logger.debug("Delete request completed successfully for database: {}, deleted {} keys",
                databaseName, successCount);
    }

    @Override
    public StreamObserver<KeyValue> batchPut(StreamObserver<PutResponse> responseObserver) {
        logger.info("Starting batch put operation");

        AtomicBoolean hasError = new AtomicBoolean(false);
        return new StreamObserver<KeyValue>() {
            private String currentDbName = null;
            private RocksDB currentDb = null;
            private WriteBatch currentBatch = null;
            private int totalKeyCount = 0;
            private List<String> processedDatabases = new ArrayList<>();

            @Override
            public void onNext(KeyValue keyValue) {
                if (hasError.get()) {
                    return;
                }

                String dbName = keyValue.getDatabaseName();
                if (currentDbName == null) {
                    currentDbName = dbName;
                    currentDb = rocksDBConfiguration.getRocksDB(currentDbName);

                    if (currentDb == null) {
                        hasError.set(true);
                        responseObserver.onError(Status.NOT_FOUND
                                .withDescription("Database not found: " + currentDbName)
                                .asRuntimeException());
                        return;
                    }

                    currentBatch = new WriteBatch();
                    if (!processedDatabases.contains(currentDbName)) {
                        processedDatabases.add(currentDbName);
                    }
                    logger.debug("Starting batch put for database: {}", currentDbName);
                } else if (!currentDbName.equals(dbName)) {
                    // If database changes, write the batch and reset for the new DB
                    try {
                        if (currentBatch.count() > 0) {
                            currentDb.write(rocksDBConfiguration.getWriteOptions(), currentBatch);
                            logger.debug("Wrote batch with {} keys to database: {}",
                                    currentBatch.count(), currentDbName);
                        }
                        currentBatch.close();
                    } catch (RocksDBException e) {
                        hasError.set(true);
                        logger.error("Error writing batch to database: {}", currentDbName, e);
                        responseObserver.onError(Status.INTERNAL
                                .withDescription("Error writing batch: " + e.getMessage())
                                .asRuntimeException());
                        return;
                    }

                    currentDbName = dbName;
                    currentDb = rocksDBConfiguration.getRocksDB(currentDbName);

                    if (currentDb == null) {
                        hasError.set(true);
                        responseObserver.onError(Status.NOT_FOUND
                                .withDescription("Database not found: " + currentDbName)
                                .asRuntimeException());
                        return;
                    }

                    currentBatch = new WriteBatch();
                    if (!processedDatabases.contains(currentDbName)) {
                        processedDatabases.add(currentDbName);
                    }
                }

                try {
                    currentBatch.put(
                            keyValue.getKey().toByteArray(),
                            keyValue.getValue().toByteArray());
                    totalKeyCount++;
                } catch (Exception e) {
                    hasError.set(true);
                    logger.error("Error adding key to batch for database: {}", currentDbName, e);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("Error adding key to batch: " + e.getMessage())
                            .asRuntimeException());
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Client error during batch put: {}", t.getMessage());
                cleanup();
                hasError.set(true);
            }

            @Override
            public void onCompleted() {
                if (hasError.get()) {
                    cleanup();
                    return;
                }

                if (currentDbName == null || currentDb == null || currentBatch == null) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("No data received in batch put stream")
                            .asRuntimeException());
                    return;
                }

                try {
                    currentDb.write(rocksDBConfiguration.getWriteOptions(), currentBatch);

                    // Get latest sequence number from the last written DB
                    long newSequenceNumber = currentDb.getLatestSequenceNumber();

                    com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                            .build();

                    PutResponse response = PutResponse.newBuilder()
                            .setStatus(status)
                            .setSuccessCount(totalKeyCount)
                            .setNewSequenceNumber(newSequenceNumber)
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    logger.info("Completed batch put for {} databases, wrote {} keys",
                            processedDatabases.size(), totalKeyCount);
                } catch (RocksDBException e) {
                    logger.error("Error writing batch to database: {}", currentDbName, e);
                    com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.ERROR)
                            .setMessage("Error writing batch: " + e.getMessage())
                            .build();

                    PutResponse response = PutResponse.newBuilder()
                            .setStatus(status)
                            .setSuccessCount(0)
                            .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } finally {
                    cleanup();
                }
            }

            private void cleanup() {
                if (currentBatch != null) {
                    currentBatch.close();
                    currentBatch = null;
                }
            }
        };
    }

    @Override
    public void getCurrentSequenceNumber(DatabaseRequest request,
                                         StreamObserver<SequenceNumberResponse> responseObserver) {
        String databaseName = request.getDatabaseName();
        logger.info("Processing getCurrentSequenceNumber request for database: {}", databaseName);

        RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
        if (rocksDB == null) {
            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                .setMessage("Database not found: " + databaseName)
                .build();
            SequenceNumberResponse response = SequenceNumberResponse.newBuilder()
                .setStatus(status)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        long sequenceNumber = rocksDB.getLatestSequenceNumber();

        com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
            .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
            .build();

        SequenceNumberResponse response = SequenceNumberResponse.newBuilder()
            .setStatus(status)
            .setSequenceNumber(sequenceNumber)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        logger.debug("GetCurrentSequenceNumber completed for database: {}, sequence: {}",
                databaseName, sequenceNumber);
    }

    @Override
    public void getFromSequence(SequenceRequest request,
            StreamObserver<KeyValueWithSequence> responseObserver) {
        String databaseName = request.getDatabaseName();
        long startSequence = request.getStartSequenceNumber();
        int limit = request.getLimit();

        logger.info("Processing getFromSequence request for database: {} from sequence: {}",
                databaseName, startSequence);

        try {
            RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
            if (rocksDB == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Database not found: " + databaseName)
                        .asRuntimeException());
                return;
            }

            long currentSequence = rocksDB.getLatestSequenceNumber();

            try (ReadOptions readOptions = new ReadOptions()) {
                readOptions.setSnapshot(rocksDB.getSnapshot());

                try (RocksIterator iterator = rocksDB.newIterator(readOptions)) {
                    int count = 0;
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                        if (limit > 0 && count >= limit) {
                            break;
                        }

                        byte[] key = iterator.key();
                        byte[] value = iterator.value();

                        KeyValueWithSequence keyValueWithSeq = KeyValueWithSequence.newBuilder()
                                .setKey(ByteString.copyFrom(key))
                                .setValue(ByteString.copyFrom(value))
                                .setSequenceNumber(currentSequence)
                                .build();

                        responseObserver.onNext(keyValueWithSeq);
                        count++;
                    }
                }
            }

            responseObserver.onCompleted();
            logger.debug("GetFromSequence completed for database: {}", databaseName);
        } catch (Exception e) {
            logger.error("Error in getFromSequence for database: {}", databaseName, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Error retrieving sequence: " + e.getMessage())
                    .asRuntimeException());
        }
    }

    // Inner class to track replication status
    private static class ReplicationSession {
        private final String replicationId;
        private final String sourceHost;
        private final int sourcePort;
        private final String sourceDb;
        private final String targetDb;
        private final long startSequenceNumber;
        private long endSequenceNumber;
        private int recordsProcessed;
        private boolean completed;
        private String error;

        public ReplicationSession(String replicationId, String sourceHost, int sourcePort,
                String sourceDb, String targetDb, long startSequenceNumber) {
            this.replicationId = replicationId;
            this.sourceHost = sourceHost;
            this.sourcePort = sourcePort;
            this.sourceDb = sourceDb;
            this.targetDb = targetDb;
            this.startSequenceNumber = startSequenceNumber;
            this.recordsProcessed = 0;
            this.completed = false;
        }

        public String getReplicationId() { return replicationId; }
        public long getStartSequenceNumber() { return startSequenceNumber; }
        public synchronized void setEndSequenceNumber(long value) { this.endSequenceNumber = value; }
        public synchronized void setRecordsProcessed(int value) { this.recordsProcessed = value; }
        public synchronized void setCompleted(boolean value) { this.completed = value; }
        public synchronized void setError(String value) { this.error = value; }
        public synchronized boolean isCompleted() { return completed; }
        public synchronized String getError() { return error; }
        public synchronized int getRecordsProcessed() { return recordsProcessed; }
        public synchronized long getEndSequenceNumber() { return endSequenceNumber; }
    }

    @Override
    public void startReplication(ReplicationRequest request,
            StreamObserver<ReplicationResponse> responseObserver) {
        String sourceHost = request.getSourceHost();
        int sourcePort = request.getSourcePort();
        String sourceDb = request.getSourceDatabaseName();
        String targetDb = request.getTargetDatabaseName();
        long startSequence = request.getStartSequenceNumber();

        logger.info("Starting replication from {}:{}/{} to local db {} from sequence {}",
                sourceHost, sourcePort, sourceDb, targetDb, startSequence);

        try {
            // Ensure the target database exists
            RocksDB targetRocksDB = rocksDBConfiguration.getRocksDB(targetDb);
            if (targetRocksDB == null) {
                com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                    .setCode(com.hitorro.rocksdbserver.proto.Status.Code.DATABASE_NOT_FOUND)
                    .setMessage("Target database not found: " + targetDb)
                    .build();
                ReplicationResponse response = ReplicationResponse.newBuilder()
                    .setStatus(status)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // Start the replication process in a separate thread
            String replicationId = generateReplicationId(sourceHost, sourcePort, sourceDb, targetDb);
            ReplicationSession replicationSession = new ReplicationSession(
                    replicationId, sourceHost, sourcePort, sourceDb, targetDb, startSequence);

            replicationSessions.put(replicationId, replicationSession);

            Thread replicationThread = new Thread(() -> {
                try {
                    performReplication(replicationSession);
                } catch (Exception e) {
                    logger.error("Replication failed: {}", replicationId, e);
                    replicationSession.setError(e.getMessage());
                } finally {
                    replicationSession.setCompleted(true);
                }
            });
            replicationThread.setName("replication-" + replicationId);
            replicationThread.setDaemon(true);
            replicationThread.start();

            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.OK)
                .build();

            ReplicationResponse response = ReplicationResponse.newBuilder()
                .setSourceSequenceNumber(startSequence)
                .setStatus(status)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.info("Replication started with ID: {}", replicationId);
        } catch (Exception e) {
            logger.error("Error starting replication", e);
            com.hitorro.rocksdbserver.proto.Status status = com.hitorro.rocksdbserver.proto.Status.newBuilder()
                .setCode(com.hitorro.rocksdbserver.proto.Status.Code.REPLICATION_ERROR)
                .setMessage("Error starting replication: " + e.getMessage())
                .build();
            ReplicationResponse response = ReplicationResponse.newBuilder()
                .setStatus(status)
                .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void streamReplication(ReplicationRequest request,
            StreamObserver<KeyValueWithSequence> responseObserver) {
        String databaseName = request.getSourceDatabaseName();
        long startSequence = request.getStartSequenceNumber();

        logger.info("Processing streamReplication request for database: {} from sequence: {}",
                databaseName, startSequence);

        try {
            RocksDB rocksDB = rocksDBConfiguration.getRocksDB(databaseName);
            if (rocksDB == null) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Database not found: " + databaseName)
                        .asRuntimeException());
                return;
            }

            long currentSequence = rocksDB.getLatestSequenceNumber();
            int count = 0;

            try (ReadOptions readOptions = new ReadOptions()) {
                try (RocksIterator iterator = rocksDB.newIterator(readOptions)) {
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                        byte[] key = iterator.key();
                        byte[] value = iterator.value();

                        KeyValueWithSequence keyValueWithSequence = KeyValueWithSequence.newBuilder()
                                .setKey(ByteString.copyFrom(key))
                                .setValue(ByteString.copyFrom(value))
                                .setSequenceNumber(currentSequence)
                                .build();

                        responseObserver.onNext(keyValueWithSequence);
                        count++;

                        // Add a small delay to prevent flooding the client
                        if (count % 1000 == 0) {
                            Thread.sleep(10);
                        }
                    }
                }
            }

            responseObserver.onCompleted();
            logger.debug("StreamReplication completed for database: {}, streamed {} key-values",
                    databaseName, count);
        } catch (Exception e) {
            logger.error("Error in streamReplication for database: {}", databaseName, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Error streaming data: " + e.getMessage())
                    .asRuntimeException());
        }
    }
    
    /**
     * Generates a unique replication ID based on source and target information
     * 
     * @param sourceHost The host of the source database
     * @param sourcePort The port of the source database
     * @param sourceDb The name of the source database
     * @param targetDb The name of the target database
     * @return A unique replication ID string
     */
    private String generateReplicationId(String sourceHost, int sourcePort, String sourceDb, String targetDb) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        return String.format("repl_%s_%d_%s_to_%s_%s", 
                sourceHost.replaceAll("[^a-zA-Z0-9]", ""), 
                sourcePort,
                sourceDb,
                targetDb,
                timestamp);
    }
    
    /**
     * Performs the actual replication process from source to target database
     * 
     * @param session The replication session containing source and target information
     * @throws Exception If any error occurs during replication
     */
    private void performReplication(ReplicationSession session) throws Exception {
        logger.info("Starting replication process: {}", session.getReplicationId());
        
        // Create a client channel to the source database
        ManagedChannel channel = null;
        RocksDBServiceGrpc.RocksDBServiceBlockingStub blockingStub = null;
        RocksDBServiceGrpc.RocksDBServiceStub asyncStub = null;
        
        try {
            // Set up the gRPC client
            channel = ManagedChannelBuilder.forAddress(session.sourceHost, session.sourcePort)
                    .usePlaintext()
                    .build();
                    
            blockingStub = RocksDBServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(REPLICATION_CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    
            asyncStub = RocksDBServiceGrpc.newStub(channel)
                    .withDeadlineAfter(REPLICATION_CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            
            // Get the target database
            RocksDB targetRocksDB = rocksDBConfiguration.getRocksDB(session.targetDb);
            if (targetRocksDB == null) {
                throw new RuntimeException("Target database not found: " + session.targetDb);
            }
            
            // Prepare replication request
            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setSourceDatabaseName(session.sourceDb)
                    .setTargetDatabaseName(session.targetDb)
                    .setStartSequenceNumber(session.startSequenceNumber)
                    .build();
            
            // Use a counter to track records processed
            AtomicInteger recordsProcessed = new AtomicInteger(0);
            
            // Use streaming replication to get data from source
            final CountDownLatch finishLatch = new CountDownLatch(1);
            
            // Create a write batch for better performance
            WriteBatch writeBatch = new WriteBatch();
            int batchSize = 0;
            final int MAX_BATCH_SIZE = 1000;
            
            Iterator<KeyValueWithSequence> replicationData = blockingStub.streamReplication(request);
            
            try {
                long lastSequenceNumber = session.startSequenceNumber;
                
                while (replicationData.hasNext()) {
                    KeyValueWithSequence kvs = replicationData.next();
                    
                    // Add to batch
                    writeBatch.put(
                            kvs.getKey().toByteArray(),
                            kvs.getValue().toByteArray());
                    
                    batchSize++;
                    lastSequenceNumber = kvs.getSequenceNumber();
                    
                    // Write batch if it reached max size
                    if (batchSize >= MAX_BATCH_SIZE) {
                        targetRocksDB.write(rocksDBConfiguration.getWriteOptions(), writeBatch);
                        recordsProcessed.addAndGet(batchSize);
                        session.setRecordsProcessed(recordsProcessed.get());
                        batchSize = 0;
                        writeBatch.close();
                        writeBatch = new WriteBatch();
                        
                        // Update session with progress
                        session.setEndSequenceNumber(lastSequenceNumber);
                        
                        // Log progress
                        if (recordsProcessed.get() % 10000 == 0) {
                            logger.info("Replication {} in progress: {} records processed",
                                    session.getReplicationId(), recordsProcessed.get());
                        }
                    }
                }
                
                // Write any remaining batch items
                if (batchSize > 0) {
                    targetRocksDB.write(rocksDBConfiguration.getWriteOptions(), writeBatch);
                    recordsProcessed.addAndGet(batchSize);
                    session.setRecordsProcessed(recordsProcessed.get());
                    session.setEndSequenceNumber(lastSequenceNumber);
                }
                
                logger.info("Replication completed: {}, processed {} records, sequence range {}-{}",
                        session.getReplicationId(), 
                        recordsProcessed.get(),
                        session.getStartSequenceNumber(),
                        session.getEndSequenceNumber());
                
                session.setCompleted(true);
            } finally {
                if (writeBatch != null) {
                    writeBatch.close();
                }
            }
            
        } catch (Exception e) {
            logger.error("Error during replication: {}", session.getReplicationId(), e);
            session.setError(e.getMessage());
            throw e;
        } finally {
            if (channel != null) {
                channel.shutdown();
                try {
                    channel.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.warn("Channel shutdown interrupted", e);
                }
            }
        }
    }
}
