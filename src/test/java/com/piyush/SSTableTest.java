package com.piyush;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class SSTableTest {

    private Path testDir;
    private Path sstablePath;

    @BeforeEach
    public void setUp() throws IOException {
        testDir = Files.createTempDirectory("sstable_test");
        sstablePath = testDir.resolve("test.db");
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Clean up test files
        Files.walk(testDir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
    }

    @Test
    public void testCreateSSTable() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));
        entries.put("key2", new Entry("key2", "value2"));
        entries.put("key3", new Entry("key3", "value3"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        assertNotNull(sstable);
        assertTrue(Files.exists(sstablePath));
        assertTrue(Files.size(sstablePath) > 0);
    }

    @Test
    public void testGetExistingKey() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));
        entries.put("key2", new Entry("key2", "value2"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        Entry result = sstable.get("key1");
        assertNotNull(result);
        assertEquals("key1", result.getKey());
        assertEquals("value1", result.getValue());
    }

    @Test
    public void testGetNonExistentKey() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        Entry result = sstable.get("nonexistent");
        assertNull(result);
    }

    @Test
    public void testGetMultipleKeys() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            entries.put("key" + i, new Entry("key" + i, "value" + i));
        }

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        for (int i = 0; i < 100; i++) {
            Entry result = sstable.get("key" + i);
            assertNotNull(result);
            assertEquals("value" + i, result.getValue());
        }
    }

    @Test
    public void testGetAllEntries() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));
        entries.put("key2", new Entry("key2", "value2"));
        entries.put("key3", new Entry("key3", "value3"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        List<Entry> allEntries = sstable.getAllEntries();
        assertEquals(3, allEntries.size());
        assertEquals("key1", allEntries.get(0).getKey());
        assertEquals("key2", allEntries.get(1).getKey());
        assertEquals("key3", allEntries.get(2).getKey());
    }

    @Test
    public void testEntriesAreSorted() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("zebra", new Entry("zebra", "value1"));
        entries.put("apple", new Entry("apple", "value2"));
        entries.put("banana", new Entry("banana", "value3"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        List<Entry> allEntries = sstable.getAllEntries();
        assertEquals("apple", allEntries.get(0).getKey());
        assertEquals("banana", allEntries.get(1).getKey());
        assertEquals("zebra", allEntries.get(2).getKey());
    }

    @Test
    public void testLoadExistingSSTable() throws IOException, ClassNotFoundException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));
        entries.put("key2", new Entry("key2", "value2"));

        SSTable.create(sstablePath, entries, 1.0);

        // Load the SSTable
        SSTable loaded = SSTable.load(sstablePath);

        Entry result = loaded.get("key1");
        assertNotNull(result);
        assertEquals("value1", result.getValue());
    }

    @Test
    public void testDeleteSSTable() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);
        assertTrue(Files.exists(sstablePath));

        sstable.delete();
        assertFalse(Files.exists(sstablePath));
    }

    @Test
    public void testGetFilePath() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        assertEquals(sstablePath, sstable.getFilePath());
    }

    @Test
    public void testTombstoneEntry() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", "value1"));
        entries.put("key2", Entry.createTombstone("key2"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        Entry result = sstable.get("key2");
        assertNotNull(result);
        assertTrue(result.isTombstone());
        assertNull(result.getValue());
    }

    @Test
    public void testEmptyValue() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key1", new Entry("key1", ""));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        Entry result = sstable.get("key1");
        assertNotNull(result);
        assertEquals("", result.getValue());
    }

    @Test
    public void testLargeValues() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        String largeValue = "v".repeat(10000);
        entries.put("key1", new Entry("key1", largeValue));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        Entry result = sstable.get("key1");
        assertNotNull(result);
        assertEquals(largeValue, result.getValue());
    }

    @Test
    public void testSpecialCharacters() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("key@#$", new Entry("key@#$", "value!@#"));
        entries.put("key\nwith\nnewlines", new Entry("key\nwith\nnewlines", "value\nwith\nnewlines"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        Entry result1 = sstable.get("key@#$");
        assertNotNull(result1);
        assertEquals("value!@#", result1.getValue());

        Entry result2 = sstable.get("key\nwith\nnewlines");
        assertNotNull(result2);
        assertEquals("value\nwith\nnewlines", result2.getValue());
    }

    @Test
    public void testBloomFilterEffectiveness() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        for (int i = 0; i < 1000; i++) {
            entries.put("key" + i, new Entry("key" + i, "value" + i));
        }

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        // Keys that don't exist should mostly return null quickly (bloom filter)
        Entry result = sstable.get("nonexistent_key");
        assertNull(result);
    }

    @Test
    public void testSparseIndex() throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        // Add enough entries to trigger sparse indexing
        for (int i = 0; i < 200; i++) {
            String key = String.format("key%05d", i);
            entries.put(key, new Entry(key, "value" + i));
        }

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);

        // Should be able to find entries efficiently using sparse index
        Entry first = sstable.get("key00000");
        Entry middle = sstable.get("key00100");
        Entry last = sstable.get("key00199");

        assertNotNull(first);
        assertNotNull(middle);
        assertNotNull(last);
        assertEquals("value0", first.getValue());
        assertEquals("value100", middle.getValue());
        assertEquals("value199", last.getValue());
    }

    @Test
    public void testPersistence() throws IOException, ClassNotFoundException {
        Map<String, Entry> entries = new TreeMap<>();
        entries.put("persistent_key", new Entry("persistent_key", "persistent_value"));

        SSTable sstable = SSTable.create(sstablePath, entries, 1.0);
        Path filePath = sstable.getFilePath();

        // "Close" the SSTable by releasing reference
        sstable = null;

        // Load it again
        SSTable reloaded = SSTable.load(filePath);
        Entry result = reloaded.get("persistent_key");

        assertNotNull(result);
        assertEquals("persistent_value", result.getValue());
    }
}