# LSM Tree Key-Value Store

A complete implementation of an LSM (Log-Structured Merge) tree-based key-value store in Java with persistence, crash recovery, and automatic compaction.

## Project Structure

```
lsm-kvstore/
├── StorageConfig.java          # Configuration settings
├── Entry.java                  # Key-value entry with metadata
├── WAL.java                    # Write-Ahead Log for durability
├── Memtable.java               # In-memory sorted table
├── BloomFilter.java            # Probabilistic membership test
├── SSTable.java                # Sorted String Table (disk)
├── CompactionManager.java      # Background compaction
└── Store.java                  # Main store interface
```

## Architecture

### Components

1. **StorageConfig**: Configuration builder pattern for all store settings
2. **Entry**: Immutable data structure representing a key-value pair with timestamp and tombstone flag
3. **WAL (Write-Ahead Log)**: Ensures durability by logging all operations before applying them
4. **Memtable**: Thread-safe in-memory sorted map (TreeMap) for fast writes
5. **BloomFilter**: Reduces disk I/O by quickly determining if a key might exist in an SSTable
6. **SSTable**: Immutable sorted files on disk with sparse index for efficient reads
7. **CompactionManager**: Merges SSTables in background to optimize read performance
8. **Store**: Main API providing put, get, delete operations

### Write Path
```
put(key, value)
    ↓
Write to WAL (durability)
    ↓
Write to Memtable
    ↓
If Memtable full → Flush to SSTable
    ↓
If SSTables exceed threshold → Trigger compaction
```

### Read Path
```
get(key)
    ↓
Check Active Memtable
    ↓
Check Immutable Memtable
    ↓
For each SSTable (newest to oldest):
    Check Bloom Filter
        ↓
    If might contain → Search SSTable using sparse index
    ↓
Return value or null
```

## Key Features

✅ **Durability**: Write-Ahead Log ensures no data loss on crash  
✅ **Crash Recovery**: Automatic recovery from WAL on restart  
✅ **Efficient Writes**: O(log n) in-memory writes to Memtable  
✅ **Optimized Reads**: Bloom filters and sparse indexes minimize disk I/O  
✅ **Automatic Compaction**: Background merging of SSTables  
✅ **Tombstone Deletion**: Proper handling of deleted keys  
✅ **Thread-Safe**: Read-write locks for concurrent access  
✅ **Configurable**: Flexible configuration for different use cases


## Usage

### Basic Example

```java
// Configure the store
StorageConfig config = new StorageConfig.Builder()
    .dataDirectory("./my_data")
    .memtableMaxSize(1024 * 1024)  // 1MB
    .compactionThreshold(4)
    .build();

// Create store
Store store = new Store(config);

// Put operations
store.put("user:1", "Alice");
store.put("user:2", "Bob");

// Get operation
String value = store.get("user:1");  // Returns "Alice"

// Delete operation
store.delete("user:2");

// Close store (flushes all data)
store.close();
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| dataDirectory | `lsm_data` | Base directory for all data |
| memtableMaxSize | 1MB | Max size before flushing to disk |
| sstableMaxSize | 10MB | Target size for SSTables |
| bloomFilterFalsePositiveRate | 1% | Bloom filter accuracy |
| compactionThreshold | 4 | Number of SSTables before compaction |

## File Formats

### WAL Format
```
key|value|timestamp|tombstone\n
```

### SSTable Format
```
[Header: bloom_size, index_size]
[Data Section: entries in sorted order]
[Bloom Filter]
[Sparse Index]
```

### Entry Format (in SSTable)
```
key_length (4 bytes)
key (variable)
value_length (4 bytes)
value (variable)
timestamp (8 bytes)
tombstone (1 byte)
```

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Put | O(log n) | In-memory TreeMap insertion |
| Get (in Memtable) | O(log n) | TreeMap lookup |
| Get (in SSTable) | O(log m + k) | Bloom filter + sparse index + scan |
| Delete | O(log n) | Writes tombstone |
| Flush | O(n log n) | Serialize sorted entries |
| Compaction | O(n log n) | Merge multiple sorted files |

Where:
- n = number of entries in Memtable
- m = number of entries in sparse index
- k = entries between sparse index points

## Limitations & Future Enhancements

### Current Limitations
- Single-threaded writes (could be parallelized)
- Simple size-tiered compaction (could use leveled)
- No compression
- String keys/values only
- No range queries
- Basic sparse index (could use B-tree)

### Potential Enhancements
1. **Leveled Compaction**: Better write amplification
2. **Compression**: Reduce disk usage (Snappy, LZ4)
3. **Range Queries**: Scan operations
4. **Block Cache**: LRU cache for frequently accessed blocks
5. **Write Batching**: Batch multiple writes for better throughput
6. **Generic Types**: Support for arbitrary key-value types
7. **Snapshot Isolation**: Point-in-time consistent reads
8. **Replication**: Multi-node support

## Testing

To test the implementation:

```java
// Test basic operations
store.put("test", "value");
assert store.get("test").equals("value");

// Test deletion
store.delete("test");
assert store.get("test") == null;

// Test persistence
store.close();
Store newStore = new Store(config);
assert newStore.get("persisted_key").equals("persisted_value");
```

## Memory Considerations

- **Memtable**: Bounded by `memtableMaxSize`
- **Bloom Filters**: ~10 bits per key in memory
- **Sparse Index**: ~1% of SSTable size in memory
- **Total Memory**: O(memtable_size + num_sstables * (bloom_size + index_size))


## License

This is an educational implementation demonstrating LSM tree concepts.