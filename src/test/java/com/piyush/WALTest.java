package com.piyush;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class WALTest {

    private Path testWalPath;
    private WAL wal;

    @BeforeEach
    public void setUp() throws IOException {
        testWalPath = Paths.get("test_wal.log");
        Files.deleteIfExists(testWalPath);
        wal = new WAL(testWalPath);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (wal != null) {
            wal.close();
        }
        Files.deleteIfExists(testWalPath);
    }

    @Test
    public void testAppendSingleEntry() throws IOException {
        Entry entry = new Entry("key1", "value1");
        wal.append(entry);

        assertTrue(Files.exists(testWalPath));
        assertTrue(Files.size(testWalPath) > 0);
    }

    @Test
    public void testAppendMultipleEntries() throws IOException {
        Entry entry1 = new Entry("key1", "value1");
        Entry entry2 = new Entry("key2", "value2");
        Entry entry3 = new Entry("key3", "value3");

        wal.append(entry1);
        wal.append(entry2);
        wal.append(entry3);

        assertTrue(Files.size(testWalPath) > 0);
    }

    @Test
    public void testRecoverEmptyWAL() throws IOException {
        List<Entry> recovered = wal.recover();
        assertTrue(recovered.isEmpty());
    }

    @Test
    public void testRecoverSingleEntry() throws IOException {
        Entry entry = new Entry("key1", "value1", 123456789L, false);
        wal.append(entry);
        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        assertEquals(1, recovered.size());
        assertEquals("key1", recovered.get(0).getKey());
        assertEquals("value1", recovered.get(0).getValue());
        assertEquals(123456789L, recovered.get(0).getTimestamp());
        assertFalse(recovered.get(0).isTombstone());

        newWal.close();
    }

    @Test
    public void testRecoverMultipleEntries() throws IOException {
        Entry entry1 = new Entry("key1", "value1", 100L, false);
        Entry entry2 = new Entry("key2", "value2", 200L, false);
        Entry entry3 = new Entry("key3", "value3", 300L, false);

        wal.append(entry1);
        wal.append(entry2);
        wal.append(entry3);
        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        assertEquals(3, recovered.size());
        assertEquals("key1", recovered.get(0).getKey());
        assertEquals("key2", recovered.get(1).getKey());
        assertEquals("key3", recovered.get(2).getKey());

        newWal.close();
    }

    @Test
    public void testRecoverTombstone() throws IOException {
        Entry tombstone = Entry.createTombstone("deletedKey");
        wal.append(tombstone);
        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        assertEquals(1, recovered.size());
        assertEquals("deletedKey", recovered.get(0).getKey());
        assertNull(recovered.get(0).getValue());
        assertTrue(recovered.get(0).isTombstone());

        newWal.close();
    }

    @Test
    public void testRecoverWithEmptyValue() throws IOException {
        Entry entry = new Entry("key1", "", 123L, false);
        wal.append(entry);
        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        assertEquals(1, recovered.size());
        assertEquals("", recovered.get(0).getValue());

        newWal.close();
    }

    @Test
    public void testClearWAL() throws IOException {
        Entry entry1 = new Entry("key1", "value1");
        Entry entry2 = new Entry("key2", "value2");

        wal.append(entry1);
        wal.append(entry2);

        long sizeBefore = Files.size(testWalPath);
        assertTrue(sizeBefore > 0);

        wal.clear();

        // After clear, file should exist but be empty or very small
        assertTrue(Files.exists(testWalPath));
        long sizeAfter = Files.size(testWalPath);
        assertTrue(sizeAfter < sizeBefore);

        // Should be able to append after clear
        Entry entry3 = new Entry("key3", "value3");
        wal.append(entry3);
    }

    @Test
    public void testRecoverAfterClear() throws IOException {
        Entry entry1 = new Entry("key1", "value1");
        wal.append(entry1);
        wal.clear();

        Entry entry2 = new Entry("key2", "value2");
        wal.append(entry2);
        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        // Should only recover entry2 (after clear)
        assertEquals(1, recovered.size());
        assertEquals("key2", recovered.get(0).getKey());

        newWal.close();
    }

    @Test
    public void testAppendWithSpecialCharacters() throws IOException {
        Entry entry = new Entry("key|with|pipes", "value|with|pipes", 123L, false);
        wal.append(entry);
        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        assertEquals(1, recovered.size());
        assertEquals("key|with|pipes", recovered.get(0).getKey());
        assertEquals("value|with|pipes", recovered.get(0).getValue());

        newWal.close();
    }

    @Test
    public void testConcurrentAppends() throws InterruptedException, IOException {
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 20; j++) {
                    try {
                        Entry entry = new Entry("key" + threadId + "_" + j, "value" + j);
                        wal.append(entry);
                    } catch (IOException e) {
                        fail("Append failed: " + e.getMessage());
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        wal.close();

        WAL newWal = new WAL(testWalPath);
        List<Entry> recovered = newWal.recover();

        assertEquals(100, recovered.size());
        newWal.close();
    }

    @Test
    public void testCreateWALInNonExistentDirectory() throws IOException {
        Path nestedPath = Paths.get("test_dir/nested/wal.log");

        try {
            WAL nestedWal = new WAL(nestedPath);
            assertTrue(Files.exists(nestedPath));
            nestedWal.close();
        } finally {
            Files.deleteIfExists(nestedPath);
            Files.deleteIfExists(nestedPath.getParent());
            Files.deleteIfExists(nestedPath.getParent().getParent());
        }
    }
}