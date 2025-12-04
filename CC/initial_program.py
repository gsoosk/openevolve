from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod
import threading



from versioned_store import VersionedKVStore

# VersionedKVStore is an in-memory versioned key-value store that supports two main operations:
# 1. get(key: str, ts: int) -> Optional[Any]: This method retrieves the committed value of a key that is visible at a given timestamp (ts).
#    It returns the latest version of the key whose begin timestamp is less than or equal to the provided timestamp.
# 2. put(writes: Dict[str, Any], commit_ts: int) -> None: This method adds new versions of keys to the store at a specified commit timestamp (commit_ts).
#    It takes a dictionary of key-value pairs to be written and appends them as new versions in the store.
# These methods are essential for implementing concurrency control mechanisms as they allow transactions to read consistent snapshots and commit changes atomically.
# IMPORTANT: YOU DO NOT NEED TO IMPLEMENT THE VersionedKVStore class. It is already implemented and could be imported. Just improt it and  use it.
# Import it as follows:
# from versioned_store import VersionedKVStore



# you can only use the following classes. Your implementation only creates new classes that inherit from below classes.
# ---------- Transactions -- DO NOT CHANGE THIS CLASS ----------

class TxStatus(Enum):
    ACTIVE = auto()
    COMMITTED = auto()
    ABORTED = auto()

@dataclass
class Transaction:
    """Per-transaction state shared with the CC algorithm."""
    txid: int # this should be unique for each transaction. 
    start_ts: int
    status: TxStatus = TxStatus.ACTIVE
    read_set: Dict[str, int] = field(default_factory=dict)   # key -> version begin_ts read
    write_set: Dict[str, Any] = field(default_factory=dict)  # key -> intended value
    meta: Dict[str, Any] = field(default_factory=dict)       # free-form for CC's internal use
@dataclass
class CommitResult:
    ok: bool
    reason: Optional[str] = None
    # CC should set commit_ts if it applies writes. May be None on abort.
    commit_ts: Optional[int] = None



# ---------- CC Plugin Interface (LLM will only generate a class that extends Concurrency Control) ----------
class ConcurrencyControl(ABC):
    """
    This is the core class that must be implemented to define a concurrency control mechanism.
    It manages transaction visibility and conflict resolution through the methods defined below.
    Implementations can not modify the underlying storage system.
    If you need to save extran information, you can implement new classes to be used in this Concurrency Control class. e.g. a class to save something on the transaction that inherits from Transaction.
    """

    @abstractmethod
    def begin(self, store: VersionedKVStore) -> Transaction:
        """Initialize and return a new Transaction object, including any necessary metadata."""

    @abstractmethod
    def read(self, txn: Transaction, key: str, store: VersionedKVStore) -> Optional[Any]:
        """
        Retrieve the value accessible to the transaction, potentially using transaction-specific data.
        If the transaction is aborted, return None.
        """

    @abstractmethod
    def write(self, txn: Transaction, key: str, value: Any, store: VersionedKVStore) -> None:
        """
        Record a write operation intent. Update the transaction's write set and any relevant metadata/transaction state.
        If the transaction is aborted, do nothing.
        """

    @abstractmethod
    def commit(self, txn: Transaction, store: VersionedKVStore) -> CommitResult:
        """
        Finalize the transaction. If successful, apply changes to the storage and return a successful CommitResult.
        If not, return a CommitResult indicating failure and the reason.
        If the transaction is aborted, return a CommitResult indicating failure and the reason.
        """

    @abstractmethod
    def rollback(self, txn: Transaction, store: VersionedKVStore) -> None:
        """Undo any changes and release resources associated with the transaction."""


# EVOLVE-BLOCK-START
class GeneratedCC(ConcurrencyControl):
    """
    Baseline concurrency control: globally serial execution using a single lock.

    This implementation is intentionally simple and conservative:
      - All transactional operations are serialized under a global reentrant lock.
      - Each transaction sees a consistent snapshot and commits atomically.
      - There are no deadlocks or intermediate-write visibility anomalies by design.

    The evaluator will run heavy concurrent workloads against this class and
    score alternative implementations that respect the same public interface.
    """

    def __init__(self) -> None:
        # A single global lock that serializes all transactional operations.
        # Using RLock lets the same thread safely re-enter if needed.
        self._lock = threading.RLock()
        self._next_txid = 0
        self._next_ts = 0

    def _alloc_txid(self) -> int:
        self._next_txid += 1
        return self._next_txid

    def _alloc_ts(self) -> int:
        self._next_ts += 1
        return self._next_ts

    def begin(self, store: VersionedKVStore) -> Transaction:
        self._lock.acquire()
        txid = self._alloc_txid()
        start_ts = self._next_ts  # snapshot at current global time
        return Transaction(txid=txid, start_ts=start_ts)

    def read(self, txn: Transaction, key: str, store: VersionedKVStore) -> Optional[Any]:
        if txn.status is not TxStatus.ACTIVE:
            return None
        val = store.get(key, txn.start_ts)
        txn.read_set.setdefault(key, txn.start_ts)
        return val

    def write(self, txn: Transaction, key: str, value: Any, store: VersionedKVStore) -> None:
        if txn.status is not TxStatus.ACTIVE:
            return
        txn.write_set[key] = value

    def commit(self, txn: Transaction, store: VersionedKVStore) -> CommitResult:
        try:
            if txn.status is not TxStatus.ACTIVE:
                return CommitResult(ok=False, reason="Transaction not active", commit_ts=None)
            commit_ts = self._alloc_ts()
            if txn.write_set:
                store.put(txn.write_set, commit_ts)
            txn.status = TxStatus.COMMITTED
            return CommitResult(ok=True, reason=None, commit_ts=commit_ts)
        finally:
            # Release the global lock so that the next transaction can proceed.
            if self._lock._is_owned():  # type: ignore[attr-defined]
                self._lock.release()

    def rollback(self, txn: Transaction, store: VersionedKVStore) -> None:
        # Discard any buffered writes and mark as aborted.
        txn.write_set.clear()
        txn.status = TxStatus.ABORTED
        # Release the global lock if this transaction was holding it.
        if self._lock._is_owned():  # type: ignore[attr-defined]
            self._lock.release()
# EVOLVE-BLOCK-END



def build_cc() -> ConcurrencyControl:
    """
    Factory used by the evaluator to construct a concurrency control instance.

    The evaluator will call this function to get a fresh CC implementation.
    """
    return GeneratedCC()