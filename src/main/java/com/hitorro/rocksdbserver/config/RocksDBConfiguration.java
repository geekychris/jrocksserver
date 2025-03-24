package com.hitorro.rocksdbserver.config;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RocksDBConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBConfiguration.class);
    
    private final RocksDBProperties properties;
    private final Map<String, RocksDB> databases = new ConcurrentHashMap<>();
    private Options options;
    private Cache cache;
    private Statistics statistics;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    
    static {
        RocksDB.loadLibrary();
    }
    
    public RocksDBConfiguration(RocksDBProperties properties) {
        this.properties = properties;
    }
    
    @PostConstruct
    public void init() throws Exception {
        // Create data directory if it doesn't exist
        Path dataDir = Paths.get(properties.getDataDir());
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
            logger.info("Created RocksDB data directory: {}", dataDir);
        }
        
        // Initialize RocksDB options
        initializeOptions();
        
        // Open any existing databases
        File[] dbDirs = dataDir.toFile().listFiles(File::isDirectory);
        if (dbDirs != null) {
            for (File dbDir : dbDirs) {
                openDatabase(dbDir.getName());
            }
        }
    }
    
    @PreDestroy
    public void cleanup() {
        logger.info("Cleaning up RocksDB resources");
        
        // Close all databases
        databases.forEach((name, db) -> {
            try {
                db.close();
                logger.debug("Closed database: {}", name);
            } catch (Exception e) {
                logger.error("Error closing database: {}", name, e);
            }
        });
        databases.clear();
        
        // Close options and other resources
        if (writeOptions != null) writeOptions.close();
        if (readOptions != null) readOptions.close();
        if (options != null) options.close();
        if (cache != null) cache.close();
        if (statistics != null) statistics.close();
        
        logger.info("RocksDB cleanup completed");
    }
    
    public synchronized RocksDB getRocksDB(String dbName) {
        RocksDB db = databases.get(dbName);
        if (db == null) {
            try {
                db = openDatabase(dbName);
            } catch (RocksDBException e) {
                logger.error("Failed to open database: {}", dbName, e);
                return null;
            }
        }
        return db;
    }
    
    private synchronized RocksDB openDatabase(String dbName) throws RocksDBException {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        
        Path dbPath = Paths.get(properties.getDataDir(), dbName);
        if (!Files.exists(dbPath)) {
            try {
                Files.createDirectories(dbPath);
            } catch (Exception e) {
                throw new RocksDBException("Failed to create database directory: " + dbPath);
            }
        }
        
        // Create a copy of options for this database
        Options dbOptions = new Options(options);
        
        RocksDB db;
        if (properties.isTransactional()) {
            TransactionDBOptions txOptions = new TransactionDBOptions();
            db = TransactionDB.open(dbOptions, txOptions, dbPath.toString());
        } else {
            db = RocksDB.open(dbOptions, dbPath.toString());
        }
        
        databases.put(dbName, db);
        logger.info("Opened database: {} at path: {}", dbName, dbPath);
        
        return db;
    }
    
    private void initializeOptions() {
        // Create Statistics object if debug logging is enabled
        if (logger.isDebugEnabled()) {
            statistics = new Statistics();
            statistics.setStatsLevel(StatsLevel.ALL);
        }
        
        // Create LRU cache
        cache = new LRUCache(properties.getCacheSize());
        
        // Configure basic options
        options = new Options()
            .setCreateIfMissing(true)
            .setMaxOpenFiles(properties.getMaxOpenFiles())
            .setMaxBackgroundJobs(properties.getMaxBackgroundJobs())
            .setMaxBackgroundCompactions(properties.getMaxBackgroundCompactions())
            .setMaxBackgroundFlushes(properties.getMaxBackgroundFlushes());

        // Set statistics only if debug logging is enabled and statistics object was created
        if (logger.isDebugEnabled() && statistics != null) {
            options.setStatistics(statistics);
        }
        
        // Configure table options
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
            .setBlockSize(properties.getBlockSize())
            .setBlockCache(cache)
            .setFilterPolicy(new BloomFilter(properties.getBloomFilterBitsPerKey()));
        
        options.setTableFormatConfig(tableConfig);
        
        // Configure write options
        options.setWriteBufferSize(properties.getWriteBufferSize())
            .setMaxWriteBufferNumber(properties.getMaxWriteBufferNumber());
        
        // Configure compression
        if (properties.isCompressionEnabled()) {
            options.setCompressionType(CompressionType.valueOf(properties.getCompressionType().toUpperCase()))
                .setBottommostCompressionType(
                    CompressionType.valueOf(properties.getBottommostCompressionType().toUpperCase()));
        } else {
            options.setCompressionType(CompressionType.NO_COMPRESSION);
        }
        
        // Configure write options
        writeOptions = new WriteOptions()
            .setSync(properties.isSync())
            .setDisableWAL(properties.isDisableWAL());

        // Configure read options
        readOptions = new ReadOptions()
            .setVerifyChecksums(true)
            .setFillCache(true);
        
        logger.info("Initialized RocksDB options");
    }
    
    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public ReadOptions getReadOptions() {
        return readOptions;
    }
}

