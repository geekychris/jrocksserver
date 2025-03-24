package com.hitorro.rocksdbserver.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "rocksdb")
public class RocksDBProperties {
    private String dataDir;
    private boolean transactional = false;
    private int maxOpenFiles = 1000;
    private int maxBackgroundJobs = 4;
    private int maxBackgroundCompactions = 4;
    private int maxBackgroundFlushes = 2;
    private int writeBufferSize = 64 * 1024 * 1024; // 64MB
    private int maxWriteBufferNumber = 4;
    private int blockSize = 4 * 1024; // 4KB
    private boolean compressionEnabled = true;
    //private String compressionType = "SNAPPY";
    private String compressionType = "SNAPPY_COMPRESSION";

    private String bottommostCompressionType = "ZSTD";
    private int bloomFilterBitsPerKey = 10;
    private long cacheSize = 8 * 1024 * 1024; // 8MB
    private boolean sync = false;
    private boolean disableWAL = false;
    private boolean statisticsEnabled = false;
    private boolean optimizeFiltersForHits = true;
    private int level0FileNumCompactionTrigger = 4;
    private int targetFileSizeBase = 64 * 1024 * 1024; // 64MB
    private int maxBytesForLevelBase = 256 * 1024 * 1024; // 256MB
    private boolean allowConcurrentMemtableWrite = true;
    private boolean enableWriteThreadAdaptiveYield = true;
    private boolean useDirectIoForFlushAndCompaction = false;
    private boolean useAdaptiveMutex = false;
    
    private GrpcProperties grpc = new GrpcProperties();
    
    // Getter and setter methods
    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    
    public boolean isTransactional() { return transactional; }
    public void setTransactional(boolean transactional) { this.transactional = transactional; }
    
    public int getMaxOpenFiles() { return maxOpenFiles; }
    public void setMaxOpenFiles(int maxOpenFiles) { this.maxOpenFiles = maxOpenFiles; }
    
    public int getMaxBackgroundJobs() { return maxBackgroundJobs; }
    public void setMaxBackgroundJobs(int maxBackgroundJobs) { this.maxBackgroundJobs = maxBackgroundJobs; }
    
    public int getMaxBackgroundCompactions() { return maxBackgroundCompactions; }
    public void setMaxBackgroundCompactions(int maxBackgroundCompactions) { 
        this.maxBackgroundCompactions = maxBackgroundCompactions; 
    }
    
    public int getMaxBackgroundFlushes() { return maxBackgroundFlushes; }
    public void setMaxBackgroundFlushes(int maxBackgroundFlushes) { 
        this.maxBackgroundFlushes = maxBackgroundFlushes; 
    }
    
    public int getWriteBufferSize() { return writeBufferSize; }
    public void setWriteBufferSize(int writeBufferSize) { this.writeBufferSize = writeBufferSize; }
    
    public int getMaxWriteBufferNumber() { return maxWriteBufferNumber; }
    public void setMaxWriteBufferNumber(int maxWriteBufferNumber) { 
        this.maxWriteBufferNumber = maxWriteBufferNumber; 
    }
    
    public int getBlockSize() { return blockSize; }
    public void setBlockSize(int blockSize) { this.blockSize = blockSize; }
    
    public boolean isCompressionEnabled() { return compressionEnabled; }
    public void setCompressionEnabled(boolean compressionEnabled) { 
        this.compressionEnabled = compressionEnabled; 
    }
    
    public String getCompressionType() { return compressionType; }
    public void setCompressionType(String compressionType) { this.compressionType = compressionType; }
    
    public String getBottommostCompressionType() { return bottommostCompressionType; }
    public void setBottommostCompressionType(String bottommostCompressionType) { 
        this.bottommostCompressionType = bottommostCompressionType; 
    }
    
    public int getBloomFilterBitsPerKey() { return bloomFilterBitsPerKey; }
    public void setBloomFilterBitsPerKey(int bloomFilterBitsPerKey) { 
        this.bloomFilterBitsPerKey = bloomFilterBitsPerKey; 
    }
    
    public long getCacheSize() { return cacheSize; }
    public void setCacheSize(long cacheSize) { this.cacheSize = cacheSize; }
    
    public boolean isSync() { return sync; }
    public void setSync(boolean sync) { this.sync = sync; }
    
    public boolean isDisableWAL() { return disableWAL; }
    public void setDisableWAL(boolean disableWAL) { this.disableWAL = disableWAL; }
    
    public boolean isStatisticsEnabled() { return statisticsEnabled; }
    public void setStatisticsEnabled(boolean statisticsEnabled) { 
        this.statisticsEnabled = statisticsEnabled; 
    }
    
    public boolean isOptimizeFiltersForHits() { return optimizeFiltersForHits; }
    public void setOptimizeFiltersForHits(boolean optimizeFiltersForHits) { 
        this.optimizeFiltersForHits = optimizeFiltersForHits; 
    }
    
    public int getLevel0FileNumCompactionTrigger() { return level0FileNumCompactionTrigger; }
    public void setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger) { 
        this.level0FileNumCompactionTrigger = level0FileNumCompactionTrigger; 
    }
    
    public int getTargetFileSizeBase() { return targetFileSizeBase; }
    public void setTargetFileSizeBase(int targetFileSizeBase) { 
        this.targetFileSizeBase = targetFileSizeBase; 
    }
    
    public int getMaxBytesForLevelBase() { return maxBytesForLevelBase; }
    public void setMaxBytesForLevelBase(int maxBytesForLevelBase) { 
        this.maxBytesForLevelBase = maxBytesForLevelBase; 
    }
    
    public boolean isAllowConcurrentMemtableWrite() { return allowConcurrentMemtableWrite; }
    public void setAllowConcurrentMemtableWrite(boolean allowConcurrentMemtableWrite) { 
        this.allowConcurrentMemtableWrite = allowConcurrentMemtableWrite; 
    }
    
    public boolean isEnableWriteThreadAdaptiveYield() { return enableWriteThreadAdaptiveYield; }
    public void setEnableWriteThreadAdaptiveYield(boolean enableWriteThreadAdaptiveYield) { 
        this.enableWriteThreadAdaptiveYield = enableWriteThreadAdaptiveYield; 
    }
    
    public boolean isUseDirectIoForFlushAndCompaction() { return useDirectIoForFlushAndCompaction; }
    public void setUseDirectIoForFlushAndCompaction(boolean useDirectIoForFlushAndCompaction) { 
        this.useDirectIoForFlushAndCompaction = useDirectIoForFlushAndCompaction; 
    }
    
    public boolean isUseAdaptiveMutex() { return useAdaptiveMutex; }
    public void setUseAdaptiveMutex(boolean useAdaptiveMutex) { 
        this.useAdaptiveMutex = useAdaptiveMutex; 
    }
    
    public GrpcProperties getGrpc() { return grpc; }
    public void setGrpc(GrpcProperties grpc) { this.grpc = grpc; }
    
    public static class GrpcProperties {
        private boolean enabled = false;
        private int port = 50051;
        private String host = "localhost";
        private int threadPoolSize = 10;
        private long keepAliveTimeMs = 60000;
        private boolean shutdownGracefully = true;
        
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        
        public int getThreadPoolSize() { return threadPoolSize; }
        public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }
        
        public long getKeepAliveTimeMs() { return keepAliveTimeMs; }
        public void setKeepAliveTimeMs(long keepAliveTimeMs) { this.keepAliveTimeMs = keepAliveTimeMs; }
        
        public boolean isShutdownGracefully() { return shutdownGracefully; }
        public void setShutdownGracefully(boolean shutdownGracefully) { 
            this.shutdownGracefully = shutdownGracefully; 
        }
    }
}
