package com.piyush;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Sorted String Table - Immutable on-disk sorted file
 * Format: [BloomFilter][Index][Data]
 * Data Format: key_length|key|value_length|value|timestamp|tombstone
 */
public class SSTable {
    private final Path filePath;
    private final BloomFilter bloomFilter;
    private final TreeMap<String, Long> sparseIndex; // Key -> file offset
    private final long dataOffset;

    private SSTable(Path filePath, BloomFilter bloomFilter, TreeMap<String, Long> sparseIndex, long dataOffset) {
        this.filePath = filePath;
        this.bloomFilter = bloomFilter;
        this.sparseIndex = sparseIndex;
        this.dataOffset = dataOffset;
    }

    /**
     * Create SSTable from memtable entries
     */
    public static SSTable create(Path filePath, Map<String, Entry> entries, double falsePositiveRate) throws IOException {
        BloomFilter bloom = new BloomFilter(entries.size(), falsePositiveRate / 100.0);
        TreeMap<String, Long> index = new TreeMap<>();

        // Write to temporary file first
        Path tempPath = filePath.getParent().resolve(filePath.getFileName() + ".tmp");

        try (RandomAccessFile raf = new RandomAccessFile(tempPath.toFile(), "rw")) {
            // Reserve space for bloom filter and index (we'll write them later)
            raf.writeLong(0); // Placeholder for bloom filter size
            raf.writeLong(0); // Placeholder for index size

            long dataStartOffset = raf.getFilePointer();
            int indexInterval = Math.max(1, entries.size() / 100); // Sparse index every 100 entries
            int count = 0;

            // Write data entries
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                Entry entry = e.getValue();
                bloom.add(entry.getKey());

                // Add to sparse index
                if (count % indexInterval == 0) {
                    index.put(entry.getKey(), raf.getFilePointer());
                }

                writeEntry(raf, entry);
                count++;
            }

            long dataEndOffset = raf.getFilePointer();

            // Write bloom filter
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bloom.writeTo(baos);
            byte[] bloomBytes = baos.toByteArray();
            raf.write(bloomBytes);
            long bloomEndOffset = raf.getFilePointer();

            // Write index
            writeIndex(raf, index);
            long indexEndOffset = raf.getFilePointer();

            // Update header with actual sizes
            raf.seek(0);
            raf.writeLong(bloomBytes.length);
            raf.writeLong(indexEndOffset - bloomEndOffset);

            raf.close();

            // Rename temp file to actual file
            Files.move(tempPath, filePath);

            return new SSTable(filePath, bloom, index, dataStartOffset);
        } catch (IOException e) {
            Files.deleteIfExists(tempPath);
            throw e;
        }
    }

    /**
     * Load existing SSTable from disk
     */
    public static SSTable load(Path filePath) throws IOException, ClassNotFoundException {
        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            // Read header
            long bloomSize = raf.readLong();
            long indexSize = raf.readLong();
            long dataOffset = raf.getFilePointer();

            // Seek to bloom filter
            raf.seek(raf.length() - bloomSize - indexSize);

            // Read bloom filter
            byte[] bloomBytes = new byte[(int) bloomSize];
            raf.readFully(bloomBytes);
            BloomFilter bloom = BloomFilter.readFrom(new ByteArrayInputStream(bloomBytes));

            // Read index
            TreeMap<String, Long> index = readIndex(raf);

            return new SSTable(filePath, bloom, index, dataOffset);
        }
    }

    /**
     * Get value for a key
     */
    public Entry get(String key) throws IOException {
        // First check bloom filter
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        // Find starting position using sparse index
        Long startOffset = sparseIndex.floorEntry(key) != null ?
                sparseIndex.floorEntry(key).getValue() : dataOffset;

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            raf.seek(startOffset);

            // Scan until we find the key or pass it
            while (raf.getFilePointer() < raf.length() - getMetadataSize()) {
                Entry entry = readEntry(raf);
                if (entry == null) break;

                int cmp = entry.getKey().compareTo(key);
                if (cmp == 0) {
                    return entry;
                } else if (cmp > 0) {
                    // Passed the key, not found
                    break;
                }
            }
        }

        return null;
    }

    /**
     * Get all entries (for compaction)
     */
    public List<Entry> getAllEntries() throws IOException {
        List<Entry> entries = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            raf.seek(dataOffset);

            while (raf.getFilePointer() < raf.length() - getMetadataSize()) {
                Entry entry = readEntry(raf);
                if (entry == null) break;
                entries.add(entry);
            }
        }

        return entries;
    }

    /**
     * Delete SSTable file
     */
    public void delete() throws IOException {
        Files.deleteIfExists(filePath);
    }

    public Path getFilePath() {
        return filePath;
    }

    private static void writeEntry(RandomAccessFile raf, Entry entry) throws IOException {
        // Write key
        byte[] keyBytes = entry.getKey().getBytes();
        raf.writeInt(keyBytes.length);
        raf.write(keyBytes);

        // Write value
        byte[] valueBytes = entry.getValue() != null ? entry.getValue().getBytes() : new byte[0];
        raf.writeInt(valueBytes.length);
        raf.write(valueBytes);

        // Write metadata
        raf.writeLong(entry.getTimestamp());
        raf.writeBoolean(entry.isTombstone());
    }

    private static Entry readEntry(RandomAccessFile raf) throws IOException {
        try {
            // Read key
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            String key = new String(keyBytes);

            // Read value
            int valueLen = raf.readInt();
            byte[] valueBytes = new byte[valueLen];
            raf.readFully(valueBytes);
            String value = valueLen > 0 ? new String(valueBytes) : null;

            // Read metadata
            long timestamp = raf.readLong();
            boolean tombstone = raf.readBoolean();

            return new Entry(key, value, timestamp, tombstone);
        } catch (EOFException e) {
            return null;
        }
    }

    private static void writeIndex(RandomAccessFile raf, TreeMap<String, Long> index) throws IOException {
        raf.writeInt(index.size());
        for (Map.Entry<String, Long> e : index.entrySet()) {
            byte[] keyBytes = e.getKey().getBytes();
            raf.writeInt(keyBytes.length);
            raf.write(keyBytes);
            raf.writeLong(e.getValue());
        }
    }

    private static TreeMap<String, Long> readIndex(RandomAccessFile raf) throws IOException {
        TreeMap<String, Long> index = new TreeMap<>();
        int size = raf.readInt();

        for (int i = 0; i < size; i++) {
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            String key = new String(keyBytes);
            long offset = raf.readLong();
            index.put(key, offset);
        }

        return index;
    }

    private long getMetadataSize() throws IOException {
        return new RandomAccessFile(filePath.toFile(), "r").readLong() +
                new RandomAccessFile(filePath.toFile(), "r").readLong() + 16;
    }
}