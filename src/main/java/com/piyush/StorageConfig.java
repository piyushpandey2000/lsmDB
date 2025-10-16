package com.piyush;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration class for the LSM Key-Value Store
 */
public class StorageConfig {
    private final Path dataDirectory;
    private final int memtableMaxSize;
    private final int sstableMaxSize;
    private final int bloomFilterFalsePositiveRate;
    private final int compactionThreshold;

    private StorageConfig(Builder builder) {
        this.dataDirectory = builder.dataDirectory;
        this.memtableMaxSize = builder.memtableMaxSize;
        this.sstableMaxSize = builder.sstableMaxSize;
        this.bloomFilterFalsePositiveRate = builder.bloomFilterFalsePositiveRate;
        this.compactionThreshold = builder.compactionThreshold;
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    public Path getWALPath() {
        return dataDirectory.resolve("wal.log");
    }

    public Path getSSTablDirectory() {
        return dataDirectory.resolve("sstables");
    }

    public int getMemtableMaxSize() {
        return memtableMaxSize;
    }

    public int getSstableMaxSize() {
        return sstableMaxSize;
    }

    public int getBloomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    public int getCompactionThreshold() {
        return compactionThreshold;
    }

    public static class Builder {
        private Path dataDirectory = Paths.get("lsm_data");
        private int memtableMaxSize = 1024 * 1024; // 1MB
        private int sstableMaxSize = 10 * 1024 * 1024; // 10MB
        private int bloomFilterFalsePositiveRate = 1; // 1%
        private int compactionThreshold = 4; // Compact when 4 SSTables exist

        public Builder dataDirectory(String path) {
            this.dataDirectory = Paths.get(path);
            return this;
        }

        public Builder memtableMaxSize(int size) {
            this.memtableMaxSize = size;
            return this;
        }

        public Builder sstableMaxSize(int size) {
            this.sstableMaxSize = size;
            return this;
        }

        public Builder bloomFilterFalsePositiveRate(int rate) {
            this.bloomFilterFalsePositiveRate = rate;
            return this;
        }

        public Builder compactionThreshold(int threshold) {
            this.compactionThreshold = threshold;
            return this;
        }

        public StorageConfig build() {
            return new StorageConfig(this);
        }
    }
}