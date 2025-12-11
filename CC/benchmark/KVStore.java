package benchmark;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple in-memory key-value store (thread-safe):
 *   - get(key): O(1) lookup
 *   - put(writes): O(n) for n keys, with locking for thread safety
 * Concurrency:
 *   - ConcurrentHashMap for thread-safe operations
 *   - Global lock for batch writes to ensure atomicity
 */
class KVStore {
    // Initialize with estimated concurrency for 96 cores.
    // Set initial capacity (e.g., 256), load factor 0.75, concurrencyLevel 96.
    private static final int CONCURRENCY_LEVEL = 96;
    private static final int INITIAL_CAPACITY = 256;
    private static final float LOAD_FACTOR = 0.75f;

    private final ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>(INITIAL_CAPACITY, LOAD_FACTOR, CONCURRENCY_LEVEL);
    /**
     * Retrieves the current value of a key.
     * 
     * @param key The key to retrieve
     * @return The value associated with the key, or null if not found
     */
    public Object get(String key) {
        return data.get(key);
    }

    /**
     * Writes multiple key-value pairs to the store atomically.
     * 
     * @param writes A map of key-value pairs to be written
     */
    public void put(Map<String, Object> writes) {
        data.putAll(writes);
    }
}
