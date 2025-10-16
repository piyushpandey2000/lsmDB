package com.piyush;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Unit tests for LSMKeyValueStore class
 */
public class StoreTest {

    private Path testDir;
    private StorageConfig config;
    private Store store;

    @BeforeEach
    public void setUp() throws IOException, ClassNotFoundException {
        testDir = Files.createTempDirectory("lsm_test");
        config = new StorageConfig.Builder()
                .dataDirectory(testDir.toString())
                .memtableMaxSize(1024) // Small size for testing
                .compactionThreshold(3)
                .build();
        store = new Store(config);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (store != null) {
            store.close();
        }
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
    public void testPutAndGet() throws IOException {
        store.put("key1", "value1");

        String result = store.get("key1");
        assertEquals("value1", result);
    }

    @Test
    public void testGetNonExistentKey() throws IOException {
        String result = store.get("nonexistent");
        assertNull(result);
    }

    @Test
    public void testPutMultipleKeys() throws IOException {
        store.put("key1", "value1");
        store.put("key2", "value2");
        store.put("key3", "value3");

        assertEquals("value1", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
    }

    @Test
    public void testPutOverwrite() throws IOException {
        store.put("key1", "value1");
        store.put("key1", "value2");

        String result = store.get("key1");
        assertEquals("value2", result);
    }

    @Test
    public void testDelete() throws IOException {
        store.put("key1", "value1");
        store.delete("key1");

        String result = store.get("key1");
        assertNull(result);
    }

    @Test
    public void testDeleteNonExistentKey() throws IOException {
        // Should not throw exception
        store.delete("nonexistent");

        String result = store.get("nonexistent");
        assertNull(result);
    }

    @Test
    public void testPutAfterDelete() throws IOException {
        store.put("key1", "value1");
        store.delete("key1");
        store.put("key1", "value2");

        String result = store.get("key1");
        assertEquals("value2", result);
    }

    @Test
    public void testMemtableFlush() throws IOException, InterruptedException {
        // Insert enough data to trigger flush
        for (int i = 0; i < 100; i++) {
            store.put("key" + i, "value" + i);
        }

        // Wait for async flush
        Thread.sleep(500);

        // Data should still be retrievable
        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, store.get("key" + i));
        }
    }

    @Test
    public void testPersistence() throws IOException, ClassNotFoundException, InterruptedException {
        store.put("persistent1", "value1");
        store.put("persistent2", "value2");
        store.close();

        // Wait a bit to ensure everything is written
        Thread.sleep(100);

        // Reopen the store
        store = new Store(config);

        assertEquals("value1", store.get("persistent1"));
        assertEquals("value2", store.get("persistent2"));
    }

    @Test
    public void testCrashRecovery() throws IOException, ClassNotFoundException, InterruptedException {
        store.put("key1", "value1");
        store.put("key2", "value2");

        // Simulate crash (don't call close)
        store = null;

        // Wait a bit
        Thread.sleep(100);

        // Reopen - should recover from WAL
        store = new Store(config);

        assertEquals("value1", store.get("key1"));
        assertEquals("value2", store.get("key2"));
    }

    @Test
    public void testNullKeyThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            store.put(null, "value");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            store.get(null);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            store.delete(null);
        });
    }

    @Test
    public void testNullValueThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            store.put("key", null);
        });
    }

    @Test
    public void testEmptyStringValue() throws IOException {
        store.put("key1", "");

        String result = store.get("key1");
        assertEquals("", result);
    }

    @Test
    public void testLargeValue() throws IOException {
        String largeValue = "v".repeat(10000);
        store.put("largeKey", largeValue);

        String result = store.get("largeKey");
        assertEquals(largeValue, result);
    }

    @Test
    public void testSpecialCharacters() throws IOException {
        store.put("key@#$%", "value!@#$%");
        store.put("key with spaces", "value with spaces");
        store.put("key\nwith\nnewlines", "value\nwith\nnewlines");

        assertEquals("value!@#$%", store.get("key@#$%"));
        assertEquals("value with spaces", store.get("key with spaces"));
        assertEquals("value\nwith\nnewlines", store.get("key\nwith\nnewlines"));
    }

    @Test
    public void testUnicodeCharacters() throws IOException {
        store.put("键", "值");
        store.put("キー", "バリュー");
        store.put("🔑", "🔓");

        assertEquals("值", store.get("键"));
        assertEquals("バリュー", store.get("キー"));
        assertEquals("🔓", store.get("🔑"));
    }

    @Test
    public void testGetStats() throws IOException {
        store.put("key1", "value1");
        store.put("key2", "value2");

        String stats = store.getStats();
        assertNotNull(stats);
        assertTrue(stats.contains("Memtable"));
        assertTrue(stats.contains("SSTable"));
    }

    @Test
    public void testConcurrentReads() throws IOException, InterruptedException {
        store.put("key1", "value1");
        store.put("key2", "value2");

        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        assertEquals("value1", store.get("key1"));
                        assertEquals("value2", store.get("key2"));
                    }
                } catch (IOException e) {
                    fail("Read failed: " + e.getMessage());
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testSequentialWrites() throws IOException {
        for (int i = 0; i < 1000; i++) {
            store.put("key" + i, "value" + i);
        }

        // Verify all writes
        for (int i = 0; i < 1000; i++) {
            assertEquals("value" + i, store.get("key" + i));
        }
    }

    @Test
    public void testMixedOperations() throws IOException, InterruptedException {
        // Mix of puts, gets, and deletes
        store.put("key1", "value1");
        assertEquals("value1", store.get("key1"));

        store.put("key2", "value2");
        store.delete("key1");
        assertNull(store.get("key1"));
        assertEquals("value2", store.get("key2"));

        store.put("key1", "value1_new");
        assertEquals("value1_new", store.get("key1"));

        // Trigger flush
        for (int i = 0; i < 100; i++) {
            store.put("bulk" + i, "value" + i);
        }

        Thread.sleep(500);

        // Verify all operations persisted correctly
        assertNull(store.get("nonexistent"));
        assertEquals("value1_new", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value50", store.get("bulk50"));
    }

    @Test
    public void testCompaction() throws IOException, InterruptedException {
        // Insert enough data to trigger multiple flushes and compaction
        for (int batch = 0; batch < 5; batch++) {
            for (int i = 0; i < 100; i++) {
                store.put("key" + batch + "_" + i, "value" + i);
            }
            Thread.sleep(200); // Allow flush to complete
        }

        // Wait for compaction
        Thread.sleep(2000);

        // Verify all data is still accessible
        for (int batch = 0; batch < 5; batch++) {
            for (int i = 0; i < 100; i++) {
                String expected = "value" + i;
                String actual = store.get("key" + batch + "_" + i);
                assertEquals(expected, actual,
                        "Failed for key" + batch + "_" + i);
            }
        }
    }

    @Test
    public void testDeletedKeysAfterCompaction() throws IOException, InterruptedException {
        // Add and delete keys
        for (int i = 0; i < 50; i++) {
            store.put("key" + i, "value" + i);
        }

        // Delete half of them
        for (int i = 0; i < 25; i++) {
            store.delete("key" + i);
        }

        // Add more to trigger flushes
        for (int i = 50; i < 200; i++) {
            store.put("key" + i, "value" + i);
        }

        Thread.sleep(2000); // Wait for compaction

        // Verify deleted keys are gone
        for (int i = 0; i < 25; i++) {
            assertNull(store.get("key" + i), "key" + i + " should be deleted");
        }

        // Verify remaining keys exist
        for (int i = 25; i < 200; i++) {
            assertEquals("value" + i, store.get("key" + i));
        }
    }

    @Test
    public void testEmptyStore() throws IOException {
        assertNull(store.get("anykey"));

        String stats = store.getStats();
        assertNotNull(stats);
    }
}