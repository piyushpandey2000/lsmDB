package com.piyush;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class EntryTest {

    @Test
    void testBasicEntryCreation() {
        Entry entry = new Entry("key1", "value1");

        assertEquals("key1", entry.getKey());
        assertEquals("value1", entry.getValue());
        assertFalse(entry.isTombstone());
        assertTrue(entry.getTimestamp() > 0);
    }

    @Test
    void testEntryWithTimestamp() {
        long timestamp = System.currentTimeMillis();
        Entry entry = new Entry("key1", "value1", timestamp, false);

        assertEquals("key1", entry.getKey());
        assertEquals("value1", entry.getValue());
        assertEquals(timestamp, entry.getTimestamp());
        assertFalse(entry.isTombstone());
    }

    @Test
    void testTombstoneEntry() {
        Entry tombstone = Entry.createTombstone("deletedKey");

        assertEquals("deletedKey", tombstone.getKey());
        assertNull(tombstone.getValue());
        assertTrue(tombstone.isTombstone());
        assertTrue(tombstone.getTimestamp() > 0);
    }

    @Test
    void testEntryComparison() {
        Entry entry1 = new Entry("apple", "value1");
        Entry entry2 = new Entry("banana", "value2");
        Entry entry3 = new Entry("apple", "value3");

        assertTrue(entry1.compareTo(entry2) < 0);
        assertTrue(entry2.compareTo(entry1) > 0);
        assertEquals(0, entry1.compareTo(entry3));
    }

    @Test
    void testEntryComparisonWithTimestamp() throws InterruptedException {
        Entry older = new Entry("key", "old");
        Thread.sleep(2); // Ensure different timestamps
        Entry newer = new Entry("key", "new");

        // Same key, newer timestamp should come first
        assertTrue(older.compareTo(newer) > 0);
        assertTrue(newer.compareTo(older) < 0);
    }

    @Test
    void testEntryToString() {
        Entry entry = new Entry("testKey", "testValue", 123456789L, false);
        String str = entry.toString();

        assertTrue(str.contains("testKey"));
        assertTrue(str.contains("testValue"));
        assertTrue(str.contains("123456789"));
        assertTrue(str.contains("false"));
    }

    @Test
    void testTombstoneToString() {
        Entry tombstone = Entry.createTombstone("deletedKey");
        String str = tombstone.toString();

        assertTrue(str.contains("deletedKey"));
        assertTrue(str.contains("tombstone=true"));
    }

    @Test
    void testNullValueEntry() {
        Entry entry = new Entry("key", null, System.currentTimeMillis(), true);

        assertNull(entry.getValue());
        assertTrue(entry.isTombstone());
    }
}