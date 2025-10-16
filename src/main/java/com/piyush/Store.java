package com.piyush;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * LSM Tree-based Key-Value Store
 * Provides put, get, and delete operations with durability and crash recovery
 */
public class Store {
    private final StorageConfig config;
    private final WAL wal;
    private Memtable activeMemtable;
    private Memtable immutableMemtable;
    private final List<SSTable> sstables;
    private final CompactionManager compactionManager;
    private final ReadWriteLock memtableLock;

    public Store(StorageConfig config) throws IOException, ClassNotFoundException {
        this.config = config;
        this.memtableLock = new ReentrantReadWriteLock();
        this.sstables = new ArrayList<>();

        // Initialize directories
        Files.createDirectories(config.getDataDirectory());
        Files.createDirectories(config.getSSTablDirectory());

        // Initialize WAL
        this.wal = new WAL(config.getWALPath());

        // Recover from WAL if exists
        this.activeMemtable = new Memtable();
        recover();

        // Load existing SSTables
        loadSSTables();

        // Initialize compaction manager
        this.compactionManager = new CompactionManager(config);
    }

    /**
     * Put a key-value pair
     */
    public void put(String key, String value) throws IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }

        Entry entry = new Entry(key, value);

        // Write to WAL first (durability)
        wal.append(entry);

        // Write to memtable
        memtableLock.writeLock().lock();
        try {
            activeMemtable.put(key, value);

            // Check if memtable is full
            if (activeMemtable.getSize() >= config.getMemtableMaxSize()) {
                rotateMemtable();
            }
        } finally {
            memtableLock.writeLock().unlock();
        }
    }

    /**
     * Get value for a key
     */
    public String get(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // Check active memtable
        memtableLock.readLock().lock();
        try {
            Entry entry = activeMemtable.get(key);
            if (entry != null) {
                return entry.isTombstone() ? null : entry.getValue();
            }

            // Check immutable memtable
            if (immutableMemtable != null) {
                entry = immutableMemtable.get(key);
                if (entry != null) {
                    return entry.isTombstone() ? null : entry.getValue();
                }
            }
        } finally {
            memtableLock.readLock().unlock();
        }

        // Check SSTables (newest to oldest)
        synchronized (sstables) {
            for (int i = sstables.size() - 1; i >= 0; i--) {
                Entry entry = sstables.get(i).get(key);
                if (entry != null) {
                    return entry.isTombstone() ? null : entry.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Delete a key
     */
    public void delete(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        Entry tombstone = Entry.createTombstone(key);

        // Write to WAL
        wal.append(tombstone);

        // Write tombstone to memtable
        memtableLock.writeLock().lock();
        try {
            activeMemtable.delete(key);

            if (activeMemtable.getSize() >= config.getMemtableMaxSize()) {
                rotateMemtable();
            }
        } finally {
            memtableLock.writeLock().unlock();
        }
    }

    /**
     * Flush all data to disk and close
     */
    public void close() throws IOException {
        // Flush active memtable
        memtableLock.writeLock().lock();
        try {
            if (!activeMemtable.isEmpty()) {
                flushMemtable(activeMemtable);
            }
            if (immutableMemtable != null && !immutableMemtable.isEmpty()) {
                flushMemtable(immutableMemtable);
            }
        } finally {
            memtableLock.writeLock().unlock();
        }

        // Close WAL
        wal.close();

        // Shutdown compaction
        compactionManager.shutdown();
    }

    /**
     * Recover from WAL after crash
     */
    private void recover() throws IOException {
        List<Entry> entries = wal.recover();

        if (!entries.isEmpty()) {
            System.out.println("Recovering " + entries.size() + " entries from WAL...");

            for (Entry entry : entries) {
                if (entry.isTombstone()) {
                    activeMemtable.delete(entry.getKey());
                } else {
                    activeMemtable.put(entry.getKey(), entry.getValue());
                }
            }

            System.out.println("Recovery complete");
        }
    }

    /**
     * Load existing SSTables from disk
     */
    private void loadSSTables() throws IOException, ClassNotFoundException {
        List<Path> sstableFiles = Files.list(config.getSSTablDirectory())
                .filter(p -> p.toString().endsWith(".db"))
                .sorted()
                .collect(Collectors.toList());

        for (Path path : sstableFiles) {
            SSTable sstable = SSTable.load(path);
            sstables.add(sstable);
        }

        if (!sstables.isEmpty()) {
            System.out.println("Loaded " + sstables.size() + " SSTables from disk");
        }
    }

    /**
     * Rotate memtable: active -> immutable, create new active
     */
    private void rotateMemtable() throws IOException {
        // If there's already an immutable memtable, flush it first
        if (immutableMemtable != null) {
            flushMemtable(immutableMemtable);
            immutableMemtable = null;
        }

        // Rotate active to immutable
        immutableMemtable = activeMemtable;
        activeMemtable = new Memtable();

        // Clear WAL since we're starting fresh
        wal.clear();

        // Flush immutable memtable in background
        flushMemtableAsync(immutableMemtable);
    }

    /**
     * Flush memtable to SSTable
     */
    private void flushMemtable(Memtable memtable) throws IOException {
        if (memtable.isEmpty()) {
            return;
        }

        Map<String, Entry> entries = memtable.getAllEntries();
        long timestamp = System.currentTimeMillis();
        Path sstablePath = config.getSSTablDirectory().resolve("sstable_" + timestamp + ".db");

        System.out.println("Flushing memtable with " + entries.size() + " entries to SSTable...");

        SSTable sstable = SSTable.create(
                sstablePath,
                entries,
                config.getBloomFilterFalsePositiveRate()
        );

        synchronized (sstables) {
            sstables.add(sstable);
        }

        System.out.println("Flush complete");

        // Trigger compaction if needed
        synchronized (sstables) {
            compactionManager.maybeCompact(new ArrayList<>(sstables));
        }
    }

    /**
     * Flush memtable asynchronously
     */
    private void flushMemtableAsync(Memtable memtable) {
        new Thread(() -> {
            try {
                flushMemtable(memtable);
                memtableLock.writeLock().lock();
                try {
                    if (immutableMemtable == memtable) {
                        immutableMemtable = null;
                    }
                } finally {
                    memtableLock.writeLock().unlock();
                }
            } catch (IOException e) {
                System.err.println("Failed to flush memtable: " + e.getMessage());
                e.printStackTrace();
            }
        }, "LSM-Flush").start();
    }

    /**
     * Get statistics about the store
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== LSM Key-Value Store Statistics ===\n");
        sb.append("Active Memtable: ").append(activeMemtable.getEntryCount())
                .append(" entries, ~").append(activeMemtable.getSize()).append(" bytes\n");

        if (immutableMemtable != null) {
            sb.append("Immutable Memtable: ").append(immutableMemtable.getEntryCount())
                    .append(" entries, ~").append(immutableMemtable.getSize()).append(" bytes\n");
        }

        synchronized (sstables) {
            sb.append("SSTables: ").append(sstables.size()).append("\n");
        }

        return sb.toString();
    }
}