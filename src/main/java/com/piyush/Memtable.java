package com.piyush;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory sorted data structure for recent writes
 * Thread-safe using read-write locks
 */
public class Memtable {
    private final NavigableMap<String, Entry> data;
    private final ReadWriteLock lock;
    private volatile int size; // Approximate size in bytes

    public Memtable() {
        this.data = new TreeMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.size = 0;
    }

    /**
     * Put a key-value pair into the memtable
     */
    public void put(String key, String value) {
        Entry entry = new Entry(key, value);
        lock.writeLock().lock();
        try {
            Entry oldEntry = data.put(key, entry);
            size += estimateSize(entry);
            if (oldEntry != null) {
                size -= estimateSize(oldEntry);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Mark a key as deleted (tombstone)
     */
    public void delete(String key) {
        Entry tombstone = Entry.createTombstone(key);
        lock.writeLock().lock();
        try {
            Entry oldEntry = data.put(key, tombstone);
            size += estimateSize(tombstone);
            if (oldEntry != null) {
                size -= estimateSize(oldEntry);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get a value by key
     */
    public Entry get(String key) {
        lock.readLock().lock();
        try {
            return data.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get approximate size in bytes
     */
    public int getSize() {
        return size;
    }

    /**
     * Check if memtable is empty
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return data.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all entries (used for flushing to SSTable)
     */
    public Map<String, Entry> getAllEntries() {
        lock.readLock().lock();
        try {
            return new TreeMap<>(data);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get number of entries
     */
    public int getEntryCount() {
        lock.readLock().lock();
        try {
            return data.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Estimate size of an entry in bytes
     */
    private int estimateSize(Entry entry) {
        int keySize = entry.getKey().length() * 2; // Java chars are 2 bytes
        int valueSize = entry.getValue() != null ? entry.getValue().length() * 2 : 0;
        int metadataSize = 8 + 1; // timestamp (8) + tombstone (1)
        return keySize + valueSize + metadataSize;
    }
}