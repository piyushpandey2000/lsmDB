package com.piyush;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.io.*;

public class BloomFilterTest {

    @Test
    public void testAddAndMightContain() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        bloom.add("key1");
        bloom.add("key2");
        bloom.add("key3");

        assertTrue(bloom.mightContain("key1"));
        assertTrue(bloom.mightContain("key2"));
        assertTrue(bloom.mightContain("key3"));
    }

    @Test
    public void testMightContainNonExistent() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        bloom.add("key1");
        bloom.add("key2");

        assertFalse(bloom.mightContain("key3"));
        assertFalse(bloom.mightContain("nonexistent"));
    }

    @Test
    public void testNoFalseNegatives() {
        BloomFilter bloom = new BloomFilter(1000, 0.01);

        for (int i = 0; i < 100; i++) {
            bloom.add("key" + i);
        }

        for (int i = 0; i < 100; i++) {
            assertTrue(bloom.mightContain("key" + i),
                    "False negative for key" + i);
        }
    }

    @Test
    public void testFalsePositiveRate() {
        int expectedElements = 1000;
        double targetFPR = 0.01; // 1%
        BloomFilter bloom = new BloomFilter(expectedElements, targetFPR);

        for (int i = 0; i < expectedElements; i++) {
            bloom.add("key" + i);
        }

        int falsePositives = 0;
        int testSize = 10000;

        for (int i = expectedElements; i < expectedElements + testSize; i++) {
            if (bloom.mightContain("key" + i)) {
                falsePositives++;
            }
        }

        double actualFPR = (double) falsePositives / testSize;

        assertTrue(actualFPR < targetFPR * 3,
                "False positive rate too high: " + actualFPR);
    }

    @Test
    public void testEmptyBloomFilter() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        assertFalse(bloom.mightContain("anykey"));
        assertFalse(bloom.mightContain("anotherkey"));
    }

    @Test
    public void testDuplicateKeys() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        bloom.add("duplicate");
        bloom.add("duplicate");
        bloom.add("duplicate");

        assertTrue(bloom.mightContain("duplicate"));
    }

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        BloomFilter original = new BloomFilter(100, 0.01);

        original.add("key1");
        original.add("key2");
        original.add("key3");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        original.writeTo(baos);
        byte[] serialized = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        BloomFilter deserialized = BloomFilter.readFrom(bais);

        assertTrue(deserialized.mightContain("key1"));
        assertTrue(deserialized.mightContain("key2"));
        assertTrue(deserialized.mightContain("key3"));
        assertFalse(deserialized.mightContain("key4"));
    }

    @Test
    public void testLargeNumberOfElements() {
        BloomFilter bloom = new BloomFilter(10000, 0.01);

        for (int i = 0; i < 10000; i++) {
            bloom.add("key" + i);
        }

        for (int i = 0; i < 10000; i++) {
            assertTrue(bloom.mightContain("key" + i));
        }
    }

    @Test
    public void testDifferentFalsePositiveRates() {
        BloomFilter bloom1 = new BloomFilter(1000, 0.001); // 0.1%
        BloomFilter bloom2 = new BloomFilter(1000, 0.1);   // 10%

        for (int i = 0; i < 1000; i++) {
            bloom1.add("key" + i);
            bloom2.add("key" + i);
        }

        int fp1 = 0, fp2 = 0;
        for (int i = 1000; i < 2000; i++) {
            if (bloom1.mightContain("key" + i)) fp1++;
            if (bloom2.mightContain("key" + i)) fp2++;
        }

        assertTrue(fp1 < fp2);
    }

    @Test
    public void testSpecialCharactersInKeys() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        bloom.add("key@#$%");
        bloom.add("key with spaces");
        bloom.add("key\nwith\nnewlines");
        bloom.add("key\twith\ttabs");

        assertTrue(bloom.mightContain("key@#$%"));
        assertTrue(bloom.mightContain("key with spaces"));
        assertTrue(bloom.mightContain("key\nwith\nnewlines"));
        assertTrue(bloom.mightContain("key\twith\ttabs"));
    }

    @Test
    public void testUnicodeKeys() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        bloom.add("键值对");
        bloom.add("키값쌍");
        bloom.add("キーバリュー");
        bloom.add("🔑🔓");

        assertTrue(bloom.mightContain("键值对"));
        assertTrue(bloom.mightContain("키값쌍"));
        assertTrue(bloom.mightContain("キーバリュー"));
        assertTrue(bloom.mightContain("🔑🔓"));
    }

    @Test
    public void testEmptyStringKey() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        bloom.add("");
        assertTrue(bloom.mightContain(""));
    }

    @Test
    public void testVeryLongKeys() {
        BloomFilter bloom = new BloomFilter(100, 0.01);

        String longKey = "k".repeat(10000);
        bloom.add(longKey);

        assertTrue(bloom.mightContain(longKey));
    }

    @Test
    public void testSerializationSize() throws IOException {
        BloomFilter bloom = new BloomFilter(1000, 0.01);

        for (int i = 0; i < 1000; i++) {
            bloom.add("key" + i);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        bloom.writeTo(baos);

        assertTrue(baos.size() < 10000, "Bloom filter too large: " + baos.size());
    }
}