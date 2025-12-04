from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import threading
from bisect import bisect_right

@dataclass
class Version:
    """A committed version. end_ts is implied by the next version's begin_ts."""
    begin_ts: int
    value: Any


class _KeyChain:
    """Per-key version chain guarded by its own lock."""
    __slots__ = ("lock", "ts", "vals")

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.ts: List[int] = []
        self.vals: List[Any] = []


class VersionedKVStore:
    """
    In-memory versioned key-value store (efficient, thread-safe):
      - get(key, ts): O(log n) via binary search on per-key timestamp list
      - put(writes, commit_ts): amortized O(1) for append; O(n) only when inserting out-of-order
    Concurrency:
      - Per-key locks to avoid global contention
      - Lightweight meta lock only for creating new key chains
    """
    def __init__(self):
        self._chains: Dict[str, _KeyChain] = {}
        self._meta_lock = threading.Lock()

    def _get_chain(self, key: str) -> _KeyChain:
        chain = self._chains.get(key)
        if chain is not None:
            return chain
        with self._meta_lock:
            chain = self._chains.get(key)
            if chain is None:
                chain = _KeyChain()
                self._chains[key] = chain
            return chain

    def get(self, key: str, ts: int) -> Optional[Any]:
        chain = self._get_chain(key)
        with chain.lock:
            if not chain.ts:
                return None
            # Find rightmost index where begin_ts <= ts
            idx = bisect_right(chain.ts, ts) - 1
            if idx >= 0:
                return chain.vals[idx]
            return None

    def put(self, writes: Dict[str, Any], commit_ts: int) -> None:
        # Lock all chains in a sorted manner before applying writes
        chains_to_lock = sorted([(self._get_chain(k), k, v) for k, v in writes.items()], key=lambda x: x[1])
        for chain, k, v in chains_to_lock:
            chain.lock.acquire()
        try:
            for chain, k, v in chains_to_lock:
                if chain.ts and commit_ts >= chain.ts[-1]:
                    # Fast-path append
                    chain.ts.append(commit_ts)
                    chain.vals.append(v)
                else:
                    # Insert while maintaining ascending order by begin_ts
                    pos = bisect_right(chain.ts, commit_ts)
                    chain.ts.insert(pos, commit_ts)
                    chain.vals.insert(pos, v)
        finally:
            for chain, _, _ in chains_to_lock:
                chain.lock.release()
