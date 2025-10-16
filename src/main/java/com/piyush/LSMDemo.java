package com.piyush;

import java.io.IOException;

/**
 * Demo application showing usage of LSM Key-Value Store
 */
public class LSMDemo {

    public static void main(String[] args) {
        try {
            // Configure the store
            StorageConfig config = new StorageConfig.Builder()
                    .dataDirectory("./lsm_demo_data")
                    .memtableMaxSize(1024 * 10) // 10KB for demo (normally much larger)
                    .bloomFilterFalsePositiveRate(1)
                    .compactionThreshold(3)
                    .build();

            // Create the store
            Store store = new Store(config);

            System.out.println("=== LSM Key-Value Store Demo ===\n");

            // Example 1: Basic put and get
            System.out.println("1. Basic Operations:");
            store.put("user:1", "Alice");
            store.put("user:2", "Bob");
            store.put("user:3", "Charlie");

            System.out.println("  user:1 = " + store.get("user:1"));
            System.out.println("  user:2 = " + store.get("user:2"));
            System.out.println("  user:3 = " + store.get("user:3"));
            System.out.println();

            // Example 2: Update existing key
            System.out.println("2. Update Operation:");
            store.put("user:1", "Alice Smith");
            System.out.println("  user:1 (updated) = " + store.get("user:1"));
            System.out.println();

            // Example 3: Delete operation
            System.out.println("3. Delete Operation:");
            store.delete("user:2");
            System.out.println("  user:2 (after delete) = " + store.get("user:2"));
            System.out.println();

            // Example 4: Insert many entries to trigger memtable flush
            System.out.println("4. Bulk Insert (triggering flush):");
            for (int i = 0; i < 100; i++) {
                store.put("key:" + i, "value:" + i);
            }
            System.out.println("  Inserted 100 entries");

            // Wait a bit for async flush
            Thread.sleep(1000);

            // Verify some entries
            System.out.println("  key:50 = " + store.get("key:50"));
            System.out.println("  key:99 = " + store.get("key:99"));
            System.out.println();

            // Example 5: Show statistics
            System.out.println("5. Store Statistics:");
            System.out.println(store.getStats());

            // Example 6: Test non-existent key
            System.out.println("6. Non-existent Key:");
            System.out.println("  nonexistent = " + store.get("nonexistent"));
            System.out.println();

            // Example 7: More inserts to trigger compaction
            System.out.println("7. More Inserts (triggering compaction):");
            for (int i = 100; i < 300; i++) {
                store.put("key:" + i, "value:" + i);
            }
            System.out.println("  Inserted 200 more entries");

            // Wait for compaction to complete
            Thread.sleep(2000);

            System.out.println("\n" + store.getStats());

            // Close the store
            store.close();
            System.out.println("Store closed successfully");

            // Example 8: Reopen and verify data persisted
            System.out.println("\n8. Crash Recovery Test:");
            System.out.println("  Reopening store...");
            Store newStore = new Store(config);

            System.out.println("  user:1 (after reopen) = " + newStore.get("user:1"));
            System.out.println("  key:50 (after reopen) = " + newStore.get("key:50"));
            System.out.println("  key:250 (after reopen) = " + newStore.get("key:250"));
            System.out.println("  user:2 (deleted, after reopen) = " + newStore.get("user:2"));

            System.out.println("\n" + newStore.getStats());

            newStore.close();
            System.out.println("\n=== Demo Complete ===");

        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}