package com.piyush;

/**
 * Represents a key-value entry with timestamp and tombstone marker
 */
public class Entry implements Comparable<Entry> {
    private final String key;
    private final String value;
    private final long timestamp;
    private final boolean tombstone;

    public Entry(String key, String value, long timestamp, boolean tombstone) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
    }

    public Entry(String key, String value) {
        this(key, value, System.currentTimeMillis(), false);
    }

    public static Entry createTombstone(String key) {
        return new Entry(key, null, System.currentTimeMillis(), true);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(Entry other) {
        int keyComp = this.key.compareTo(other.key);
        if (keyComp != 0) {
            return keyComp;
        }
        // For same key, newer timestamp comes first
        return Long.compare(other.timestamp, this.timestamp);
    }

    @Override
    public String toString() {
        return String.format("Entry{key='%s', value='%s', ts=%d, tombstone=%b}",
                key, value, timestamp, tombstone);
    }
}