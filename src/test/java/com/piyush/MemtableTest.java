package com.piyush;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Map;

public class MemtableTest {

    private Memtable memtable;

    @BeforeEach
    public void setUp() {
        memtable = new Memtable();
    }

    @Test
    public void testPutAndGet() {
        memtable.put("key1", "value1");

        Entry entry = memtable.get("key1");
        assertNotNull(entry);
        assertEquals("key1", entry.getKey());
        assertEquals("value1", entry.getValue());
        assertFalse(entry.isTombstone());
    }

    @Test
    public void testGetNonExistentKey() {
        Entry entry = memtable.get("nonexistent");
        assertNull(entry);
    }

    @Test
    public void testPutMultipleEntries() {
        memtable.put("key1", "value1");
        memtable.put("key2", "value2");
        memtable.put("key3", "value3");

        assertEquals("value1", memtable.get("key1").getValue());
        assertEquals("value2", memtable.get("key2").getValue());
        assertEquals("value3", memtable.get("key3").getValue());
        assertEquals(3, memtable.getEntryCount());
    }

    @Test
    public void testPutOverwrite() {
        memtable.put("key1", "value1");
        memtable.put("key1", "value2");

        Entry entry = memtable.get("key1");
        assertEquals("value2", entry.getValue());
        assertEquals(1, memtable.getEntryCount());
    }

    @Test
    public void testDelete() {
        memtable.put("key1", "value1");
        memtable.delete("key1");

        Entry entry = memtable.get("key1");
        assertNotNull(entry);
        assertTrue(entry.isTombstone());
        assertNull(entry.getValue());
    }

    @Test
    public void testDeleteNonExistentKey() {
        memtable.delete("nonexistent");

        Entry entry = memtable.get("nonexistent");
        assertNotNull(entry);
        assertTrue(entry.isTombstone());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(memtable.isEmpty());

        memtable.put("key1", "value1");
        assertFalse(memtable.isEmpty());
    }

    @Test
    public void testGetSize() {
        int initialSize = memtable.getSize();
        assertEquals(0, initialSize);

        memtable.put("key1", "value1");
        assertTrue(memtable.getSize() > 0);
    }

    @Test
    public void testGetSizeAfterOverwrite() {
        memtable.put("key1", "short");
        int sizeAfterFirst = memtable.getSize();

        memtable.put("key1", "much longer value");
        int sizeAfterSecond = memtable.getSize();

        assertTrue(sizeAfterSecond > sizeAfterFirst);
    }

    @Test
    public void testGetAllEntries() {
        memtable.put("key1", "value1");
        memtable.put("key2", "value2");
        memtable.put("key3", "value3");

        Map<String, Entry> entries = memtable.getAllEntries();
        assertEquals(3, entries.size());
        assertTrue(entries.containsKey("key1"));
        assertTrue(entries.containsKey("key2"));
        assertTrue(entries.containsKey("key3"));
    }

    @Test
    public void testGetAllEntriesReturnsSnapshot() {
        memtable.put("key1", "value1");
        Map<String, Entry> entries = memtable.getAllEntries();

        // Modify memtable after getting entries
        memtable.put("key2", "value2");

        // Original snapshot should be unchanged
        assertEquals(1, entries.size());
        assertEquals(2, memtable.getEntryCount());
    }

    @Test
    public void testGetEntryCount() {
        assertEquals(0, memtable.getEntryCount());

        memtable.put("key1", "value1");
        assertEquals(1, memtable.getEntryCount());

        memtable.put("key2", "value2");
        assertEquals(2, memtable.getEntryCount());

        memtable.put("key1", "updated");
        assertEquals(2, memtable.getEntryCount()); // Overwrite doesn't increase count
    }

    @Test
    public void testConcurrentReads() throws InterruptedException {
        memtable.put("key1", "value1");

        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    Entry entry = memtable.get("key1");
                    assertNotNull(entry);
                    assertEquals("value1", entry.getValue());
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testConcurrentWrites() throws InterruptedException {
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    memtable.put("key" + threadId + "_" + j, "value" + j);
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(1000, memtable.getEntryCount());
    }

    @Test
    public void testEntriesAreSorted() {
        memtable.put("zebra", "value1");
        memtable.put("apple", "value2");
        memtable.put("banana", "value3");

        Map<String, Entry> entries = memtable.getAllEntries();
        String[] keys = entries.keySet().toArray(new String[0]);

        assertEquals("apple", keys[0]);
        assertEquals("banana", keys[1]);
        assertEquals("zebra", keys[2]);
    }
}