package com.piyush;

import java.io.*;
import java.util.BitSet;

/**
 * Bloom Filter for probabilistic membership testing
 * Reduces disk reads by quickly determining if a key might exist in an SSTable
 */
public class BloomFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BitSet bitSet;
    private final int bitSetSize;
    private final int numHashFunctions;

    public BloomFilter(int expectedElements, double falsePositiveRate) {
        // Calculate optimal bit set size
        this.bitSetSize = calculateBitSetSize(expectedElements, falsePositiveRate);
        this.bitSet = new BitSet(bitSetSize);

        // Calculate optimal number of hash functions
        this.numHashFunctions = calculateNumHashFunctions(bitSetSize, expectedElements);
    }

    /**
     * Add a key to the bloom filter
     */
    public void add(String key) {
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(key, i);
            bitSet.set(Math.abs(hash % bitSetSize));
        }
    }

    /**
     * Check if a key might exist (no false negatives, possible false positives)
     */
    public boolean mightContain(String key) {
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(key, i);
            if (!bitSet.get(Math.abs(hash % bitSetSize))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Calculate optimal bit set size
     * m = -n * ln(p) / (ln(2)^2)
     */
    private int calculateBitSetSize(int n, double p) {
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * Calculate optimal number of hash functions
     * k = (m / n) * ln(2)
     */
    private int calculateNumHashFunctions(int m, int n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Generate hash using murmur3-like approach
     */
    private int hash(String key, int seed) {
        int h = seed;
        for (int i = 0; i < key.length(); i++) {
            h = 31 * h + key.charAt(i);
        }
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    /**
     * Serialize bloom filter to file
     */
    public void writeTo(OutputStream out) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeInt(bitSetSize);
            oos.writeInt(numHashFunctions);
            oos.writeObject(bitSet);
        }
    }

    /**
     * Deserialize bloom filter from file
     */
    public static BloomFilter readFrom(InputStream in) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(in)) {
            int bitSetSize = ois.readInt();
            int numHashFunctions = ois.readInt();
            BitSet bitSet = (BitSet) ois.readObject();

            return new BloomFilter(bitSetSize, numHashFunctions, bitSet);
        }
    }

    private BloomFilter(int bitSetSize, int numHashFunctions, BitSet bitSet) {
        this.bitSetSize = bitSetSize;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = bitSet;
    }
}