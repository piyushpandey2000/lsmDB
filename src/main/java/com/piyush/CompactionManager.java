package com.piyush;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages compaction of SSTables to reduce read amplification
 * Uses size-tiered compaction strategy
 */
public class CompactionManager {
    private final Path sstableDir;
    private final StorageConfig config;
    private final ExecutorService compactionExecutor;
    private volatile boolean running;

    public CompactionManager(StorageConfig config) {
        this.config = config;
        this.sstableDir = config.getSSTablDirectory();
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "LSM-Compaction");
            t.setDaemon(true);
            return t;
        });
        this.running = true;
    }

    /**
     * Trigger compaction if threshold is met
     */
    public void maybeCompact(List<SSTable> sstables) {
        if (sstables.size() >= config.getCompactionThreshold()) {
            compactionExecutor.submit(() -> {
                try {
                    compact(sstables);
                } catch (Exception e) {
                    System.err.println("Compaction failed: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
    }

    /**
     * Perform compaction of SSTables
     * Merges multiple SSTables into one, removing tombstones and keeping latest values
     */
    private synchronized void compact(List<SSTable> sstables) throws IOException, ClassNotFoundException {
        if (sstables.isEmpty() || !running) {
            return;
        }

        System.out.println("Starting compaction of " + sstables.size() + " SSTables...");

        // Collect all entries from all SSTables
        Map<String, Entry> mergedEntries = new TreeMap<>();

        for (SSTable sstable : sstables) {
            List<Entry> entries = sstable.getAllEntries();
            for (Entry entry : entries) {
                String key = entry.getKey();
                Entry existing = mergedEntries.get(key);

                // Keep the entry with the latest timestamp
                if (existing == null || entry.getTimestamp() > existing.getTimestamp()) {
                    // Skip tombstones during compaction (garbage collection)
                    if (!entry.isTombstone()) {
                        mergedEntries.put(key, entry);
                    } else {
                        mergedEntries.remove(key);
                    }
                }
            }
        }

        if (mergedEntries.isEmpty()) {
            // Just delete old SSTables if no entries remain
            for (SSTable sstable : sstables) {
                sstable.delete();
            }
            System.out.println("Compaction complete: all entries were tombstones, deleted SSTables");
            return;
        }

        // Create new compacted SSTable
        long timestamp = System.currentTimeMillis();
        Path compactedPath = sstableDir.resolve("sstable_" + timestamp + ".db");
        SSTable compactedSSTable = SSTable.create(
                compactedPath,
                mergedEntries,
                config.getBloomFilterFalsePositiveRate()
        );

        // Delete old SSTables
        for (SSTable sstable : sstables) {
            sstable.delete();
        }

        System.out.println("Compaction complete: merged " + sstables.size() +
                " SSTables into 1 with " + mergedEntries.size() + " entries");
    }

    /**
     * Shutdown compaction manager
     */
    public void shutdown() {
        running = false;
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}