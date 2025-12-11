package benchmark;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Transaction status enum.
 */
enum TxStatus {
    ACTIVE,
    COMMITTED,
    ABORTED
}

/**
 * Per-transaction state shared with the CC algorithm.
 * Uses ConcurrentHashMap for thread-safe read/write set operations.
 */
class Transaction {
    public final int txid;  // This should be unique for each transaction
    public final int startTs;
    public volatile TxStatus status = TxStatus.ACTIVE;
    public final ConcurrentHashMap<String, Object> readSet = new ConcurrentHashMap<>();    // key -> value read
    public final ConcurrentHashMap<String, Object> writeSet = new ConcurrentHashMap<>();   // key -> intended value
    public final ConcurrentHashMap<String, Object> meta = new ConcurrentHashMap<>();       // free-form for CC's internal use

    public Transaction(int txid, int startTs) {
        this.txid = txid;
        this.startTs = startTs;
    }
}

/**
 * Result of a commit operation.
 */
class CommitResult {
    public final boolean ok;
    public final String reason;
    public final Integer commitTs;  // CC should set commit_ts if it applies writes. May be null on abort.

    public CommitResult(boolean ok) {
        this(ok, null, null);
    }

    public CommitResult(boolean ok, String reason) {
        this(ok, reason, null);
    }

    public CommitResult(boolean ok, String reason, Integer commitTs) {
        this.ok = ok;
        this.reason = reason;
        this.commitTs = commitTs;
    }

    public static CommitResult success(int commitTs) {
        return new CommitResult(true, null, commitTs);
    }

    public static CommitResult failure(String reason) {
        return new CommitResult(false, reason, null);
    }
}

/**
 * CC Plugin Interface.
 * This is the core class that must be implemented to define a concurrency control mechanism.
 * It manages transaction visibility and conflict resolution through the methods defined below.
 * Implementations cannot modify the underlying storage system.
 */
abstract class ConcurrencyControl {
    
    /**
     * Initialize and return a new Transaction object, including any necessary metadata.
     */
    public abstract Transaction begin(KVStore store);

    /**
     * Retrieve the value accessible to the transaction, potentially using transaction-specific data.
     * If the transaction is aborted, return null.
     */
    public abstract Object read(Transaction txn, String key, KVStore store);

    /**
     * Record a write operation intent. Update the transaction's write set and any relevant metadata/transaction state.
     * If the transaction is aborted, do nothing.
     */
    public abstract void write(Transaction txn, String key, Object value, KVStore store);

    /**
     * Finalize the transaction. If successful, apply changes to the storage and return a successful CommitResult.
     * If not, return a CommitResult indicating failure and the reason.
     * If the transaction is aborted, return a CommitResult indicating failure and the reason.
     */
    public abstract CommitResult commit(Transaction txn, KVStore store);

    /**
     * Undo any changes and release resources associated with the transaction.
     */
    public abstract void rollback(Transaction txn, KVStore store);
}

/**
 * Transaction class for Two-Phase Locking.
 * Tracks the set of locks held by this transaction.
 */
class TwoPLTransaction extends Transaction {
    // Keys where we hold a shared (read) lock
    public final Set<String> sharedLocks = ConcurrentHashMap.newKeySet();
    // Keys where we hold an exclusive (write) lock
    public final Set<String> exclusiveLocks = ConcurrentHashMap.newKeySet();

    public TwoPLTransaction(int txid, int startTs) {
        super(txid, startTs);
    }
}

class GeneratedCC extends ConcurrencyControl {
    private final AtomicInteger txidCounter = new AtomicInteger(0);
    private final AtomicInteger timestampTicker = new AtomicInteger(1);
    private final AtomicInteger lastCommitTs = new AtomicInteger(0);
    
    // Lock manager: maps key -> lock entry
    private final ConcurrentHashMap<String, LockEntry> lockTable = new ConcurrentHashMap<>();
    
    // Global lock for commit serialization
    private final ReentrantLock commitLock = new ReentrantLock();

    private LockEntry getLockEntry(String key) {
        return lockTable.computeIfAbsent(key, k -> new LockEntry());
    }


    @Override
    public Transaction begin(KVStore store) {
        int txid = txidCounter.getAndIncrement();
        int startTs = lastCommitTs.get();
        return new TwoPLTransaction(txid, startTs);
    }

    @Override
    public Object read(Transaction txn, String key, KVStore store) {
        TwoPLTransaction tplTxn = (TwoPLTransaction) txn;

        return store.get(key);
    }

    @Override
    public void write(Transaction txn, String key, Object value, KVStore store) {
        TwoPLTransaction tplTxn = (TwoPLTransaction) txn;

        txn.writeSet.put(key, value);
    }

    @Override
    public CommitResult commit(Transaction txn, KVStore store) {
        TwoPLTransaction tplTxn = (TwoPLTransaction) txn;

        // if (txn.status == TxStatus.COMMITTED) {
        //     Integer commitTs = (Integer) txn.meta.get("commit_ts");
        //     return new CommitResult(true, null, commitTs);
        // }
        // if (txn.status == TxStatus.ABORTED) {
        //     return CommitResult.failure("Transaction was already aborted.");
        // }


        try {
            // Double-check status under lock
            if (txn.status != TxStatus.ACTIVE) {
                return CommitResult.failure("Transaction is no longer active.");
            }

            // If no writes, just commit as read-only
            if (txn.writeSet.isEmpty()) {
                txn.status = TxStatus.COMMITTED;
                int currentTs = lastCommitTs.get();
                txn.meta.put("commit_ts", currentTs);
                return new CommitResult(true, null, currentTs);
            }

            // Get next commit timestamp
            int commitTs = timestampTicker.getAndIncrement();
            while (commitTs <= lastCommitTs.get()) {
                commitTs = timestampTicker.getAndIncrement();
            }

            // Apply writes to store
            store.put(new HashMap<>(txn.writeSet));
            lastCommitTs.set(commitTs);

            txn.status = TxStatus.COMMITTED;

            return CommitResult.success(commitTs);
        } finally {

        }
    }

    @Override
    public void rollback(Transaction txn, KVStore store) {
        TwoPLTransaction tplTxn = (TwoPLTransaction) txn;

        if (txn.status == TxStatus.ABORTED) {
            return;
        }

        txn.status = TxStatus.ABORTED;
        txn.readSet.clear();
        txn.writeSet.clear();

        // Release all locks
    }

    /**
     * Factory method used by the evaluator to construct a concurrency control instance.
     * The evaluator will call this function to get a fresh CC implementation.
     */
    public static ConcurrencyControl buildCc() {
        return new GeneratedCC();
    }
}
