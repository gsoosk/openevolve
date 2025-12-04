"""
Evaluator for the HakiCC concurrency control example.

This evaluator:
  - Loads the candidate concurrency control implementation from initial_program.py
  - Constructs a CC instance via build_cc() or by finding a concrete subclass
    of ConcurrencyControl
  - Runs several concurrent workloads and builds a precedence graph from the
    actual get/put operations hitting the VersionedKVStore
  - Checks for conflict-serializability (no cycles) and for lack of stalls
  - Reports metrics such as correctness and throughput in an EvaluationResult
    that OpenEvolve can optimize.
"""

import os
import sys
import threading
import time
import random
import traceback
import importlib.util
import inspect
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict, deque

from versioned_store import VersionedKVStore
from openevolve.evaluation_result import EvaluationResult

# Type-only imports to mirror the program interface without creating a hard
# runtime dependency on the initial_program module layout.
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from initial_program import Transaction, CommitResult  # noqa: F401

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Op:
    seq: int
    txid: int
    kind: str  # 'R' or 'W'
    key: str


class MockKVStore(VersionedKVStore):
    """
    Store wrapper that logs get/put operations at the time they actually hit storage.
    Uses thread-local binding to attribute operations to the current transaction ID.
    """
    def __init__(self, on_get, on_put, get_txid):
        super().__init__()
        self._on_get = on_get
        self._on_put = on_put
        self._get_txid = get_txid
        self._tls = threading.local()

    def bind_txid(self, txid: int) -> None:
        setattr(self._tls, "txid", txid)

    def current_txid(self) -> Optional[int]:
        return getattr(self._tls, "txid", None)

    # Override
    def get(self, key: str, ts: int) -> Optional[int]:
        chain = self._get_chain(key)
        with chain.lock:
            val = super().get(key, ts)
            txid = self.current_txid()
            if txid is None:
                txid = self._get_txid()
            if txid is not None:
                self._on_get(txid, key)
            return val
    # Override
    def put(self, writes: Dict[str, int], commit_ts: int) -> None:
        # First, lock all chains in a sorted manner
        chains_to_lock = sorted([(self._get_chain(k), k) for k in writes.keys()], key=lambda x: x[1])
        for chain, _ in chains_to_lock:
            chain.lock.acquire()
        try:
            txid = self.current_txid()
            if txid is None:
                txid = self._get_txid()
            # Log per-key writes before applying
            if txid is not None:
                for k in writes.keys():
                    self._on_put(txid, k)
            super().put(writes, commit_ts)
        finally:
            for chain, _ in chains_to_lock:
                chain.lock.release()


class ConcurrentRunner:
    """
    Runs a concurrent workload against a single store and a CC instance,
    logging read/write operations for building the precedence graph.
    Only committed transactions' operations are considered for serializability.

    Tracks per-thread progress using pre-assigned slots and thread-local indices
    to allow lock-free progress updates on the hot path.
    """

    def __init__(self, cc_factory, num_workers: int = 8, duration_s: float = 1.0, stall_threshold_s: float = 0.5):
        self.cc_factory = cc_factory
        self.num_workers = num_workers
        self.duration_s = duration_s
        self.stall_threshold_s = stall_threshold_s

        self._ops: List[Op] = []
        self._ops_lock = threading.Lock()
        self._seq = 0

        def next_seq() -> int:
            # there should be a lock on ops_lock before the execution of this function. to ensure operation ordering. 
            self._seq += 1
            return self._seq

        # Callbacks used by MockKVStore
        def on_get(txid: int, key: str) -> None:
            with self._ops_lock:
                self._ops.append(Op(seq=next_seq(), txid=txid, kind='R', key=key))

        def on_put(txid: int, key: str) -> None:
            with self._ops_lock:
                self._ops.append(Op(seq=next_seq(), txid=txid, kind='W', key=key))

        def get_txid() -> Optional[int]:
            # Fallback: use thread-local bound txid from runner if store has none
            return getattr(self._tls, "txn_txid", None)

        self._tls = threading.local()
        self.store = MockKVStore(on_get=on_get, on_put=on_put, get_txid=get_txid)
        # The factory returns a fresh CC instance; the evaluator is responsible
        # for ensuring that this is the implementation under test.
        self.cc = self.cc_factory()

        self._committed: Set[int] = set()
        self._aborted: Set[int] = set()

        # Progress tracking
        self._progress_tls = threading.local()      # each thread stores its slot index here
        self._progress_slots: List[float] = [0.0] * self.num_workers
        self._slot_for_tid: Dict[int, int] = {}
        self._slot_lock = threading.Lock()
        self._next_slot = 0
        # Error propagation
        self._error = None
        self._error_tb = None
        self._error_lock = threading.Lock()

    def init_store(self, initializer) -> None:
        """Initialize the store with data before running workloads.
        
        Args:
            initializer: A callable that takes the store and populates it with data.
        """
        if initializer is not None:
            initializer(self.store)

    def run(self, workload) -> Tuple[List[Op], Set[int], Set[int]]:
        stop = threading.Event()
        threads: List[threading.Thread] = []
        start_time = time.time()

        def worker() -> None:
            tid = threading.get_ident()
            # Assign a slot for this thread once
            with self._slot_lock:
                slot = self._next_slot
                self._next_slot += 1
                self._slot_for_tid[tid] = slot
                setattr(self._progress_tls, "slot", slot)
                self._progress_slots[slot] = time.time()
            while not stop.is_set():
                try:
                    try:
                        txn = self.cc.begin(self.store)
                        # Bind transaction ID for this thread in both runner and store
                        setattr(self._tls, "txn_txid", txn.txid)
                        self.store.bind_txid(txn.txid)
                        workload(self, txn)
                    except Exception as e:
                        logger.exception("Error during transaction processing")
                        with self._error_lock:
                            if self._error is None:
                                self._error = e
                                self._error_tb = traceback.format_exc()
                                stop.set()
                        break
                    finally:
                        # Clear binding after workload iteration
                        setattr(self._tls, "txn_txid", None)
                        self.store.bind_txid(-1)
                except Exception as e:
                    # On exception, still update progress to avoid false stalls
                    slot_idx = getattr(self._progress_tls, "slot", None)
                    if slot_idx is not None and 0 <= slot_idx < len(self._progress_slots):
                        self._progress_slots[slot_idx] = time.time()
                    logger.exception("Unhandled error in worker")
                    with self._error_lock:
                        if self._error is None:
                            self._error = e
                            self._error_tb = traceback.format_exc()
                            stop.set()
                    break

        for _ in range(self.num_workers):
            t = threading.Thread(target=worker, daemon=True)
            threads.append(t)
            t.start()

        # Wait for duration or early-exit on first error
        deadline = start_time + self.duration_s
        while time.time() < deadline and not stop.is_set():
            if self._error is not None:
                break
            time.sleep(0.01)
        stop.set()
        for t in threads:
            t.join(timeout=1)
        # If any worker reported an error, surface it to caller
        if self._error is not None:
            raise RuntimeError(f"ConcurrentRunner failed with error: {self._error}\n{self._error_tb or ''}")
        # Filter ops to only those from committed transactions
        with self._ops_lock:
            committed_ops = [op for op in self._ops if op.txid in self._committed]

        # Detect stalled threads: no progress update within stall_threshold
        now = time.time()
        stalled: Set[int] = set()
        for t in threads:
            tid = t.ident
            if tid is None:
                continue
            slot = self._slot_for_tid.get(tid)
            if slot is None:
                continue
            last = self._progress_slots[slot]
            if now - last > self.stall_threshold_s:
                stalled.add(tid)
        return committed_ops, set(self._committed), stalled

    # ---------- Logging wrappers ----------

    def _touch_progress(self) -> None:
        slot_idx = getattr(self._progress_tls, "slot", None)
        if slot_idx is not None and 0 <= slot_idx < len(self._progress_slots):
            self._progress_slots[slot_idx] = time.time()

    def log_read(self, txn: "Transaction", key: str) -> Optional[int]:
        # No direct op logging here; store will log on actual get
        val = self.cc.read(txn, key, self.store)
        self._touch_progress()
        return val if isinstance(val, int) else (val if val is not None else None)

    def log_write(self, txn: "Transaction", key: str, value: int) -> None:
        # No direct op logging here; store will log on actual put (likely at commit)
        self.cc.write(txn, key, value, self.store)
        self._touch_progress()

    def log_commit(self, txn: "Transaction") -> "CommitResult":
        # Bind txn id in case CC performs store IO during commit
        setattr(self._tls, "txn_txid", txn.txid)
        self.store.bind_txid(txn.txid)
        res = self.cc.commit(txn, self.store)
        if res.ok:
            self._committed.add(txn.txid)
        else:
            self._aborted.add(txn.txid)
        self._touch_progress()
        return res

    def log_rollback(self, txn: "Transaction") -> None:
        self.cc.rollback(txn, self.store)
        self._aborted.add(txn.txid)
        self._touch_progress()


# ---------- Precedence graph (efficient) ----------

@dataclass
class _KeyState:
    last_writer: Optional[int] = None
    last_writer_seq: Optional[int] = None
    readers: Dict[int, int] = field(default_factory=dict)  # txid -> first read seq in this epoch


def build_precedence_graph_fast(ops: List[Op]) -> Tuple[Dict[int, Set[int]], Dict[Tuple[int, int], List[Dict[str, int]]]]:
    """
    Linear in ops + edges.
    Assumes ops are only from committed txns and in increasing seq order.
    Returns (graph, evidence) where evidence[(u,v)] is a list of dicts with
    {key, cause, from_seq, to_seq}; cause: 0=W→R, 1=W→W, 2=R→W.
    """
    graph: Dict[int, Set[int]] = defaultdict(set)
    evidence: Dict[Tuple[int, int], List[Dict[str, int]]] = defaultdict(list)

    # ensure all txids appear as nodes
    for op in ops:
        graph.setdefault(op.txid, set())

    state: Dict[str, _KeyState] = {}

    for op in ops:
        ks = state.setdefault(op.key, _KeyState())
        if op.kind == 'R':
            lw, lwseq = ks.last_writer, ks.last_writer_seq
            if lw is not None and lw != op.txid:
                graph[lw].add(op.txid)            # W→R
                evidence[(lw, op.txid)].append({"key": op.key, "cause": 0, "from_seq": lwseq or -1, "to_seq": op.seq})
            ks.readers.setdefault(op.txid, op.seq)  # dedup
        else:  # 'W'
            lw, lwseq = ks.last_writer, ks.last_writer_seq
            if lw is not None and lw != op.txid:
                graph[lw].add(op.txid)            # W→W
                evidence[(lw, op.txid)].append({"key": op.key, "cause": 1, "from_seq": lwseq or -1, "to_seq": op.seq})
            for r, rseq in ks.readers.items():
                if r != op.txid:
                    graph[r].add(op.txid)         # R→W
                    evidence[(r, op.txid)].append({"key": op.key, "cause": 2, "from_seq": rseq, "to_seq": op.seq})
            ks.last_writer = op.txid
            ks.last_writer_seq = op.seq
            ks.readers.clear()
    return graph, evidence


def has_cycle_kahn(graph: Dict[int, Set[int]]) -> bool:
    indeg = {u: 0 for u in graph}
    for u, vs in graph.items():
        for v in vs:
            indeg[v] = indeg.get(v, 0) + 1
            if v not in graph:
                graph[v] = set()
    q = deque([u for u, d in indeg.items() if d == 0])
    seen = 0
    while q:
        u = q.popleft()
        seen += 1
        for v in graph[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    return seen != len(indeg)


def explain_cycle_kahn(graph: Dict[int, Set[int]], evidence: Dict[Tuple[int, int], List[Dict[str, int]]], ops: List[Op]) -> str:
    # Kahn to find remaining subgraph nodes
    indeg = {u: 0 for u in graph}
    for u, vs in graph.items():
        for v in vs:
            indeg[v] = indeg.get(v, 0) + 1
            if v not in graph:
                graph[v] = set()
    q = deque([u for u, d in indeg.items() if d == 0])
    while q:
        u = q.popleft()
        for v in graph[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    remaining = {u for u, d in indeg.items() if d > 0}
    if not remaining:
        return "Cycle present but could not isolate remaining subgraph."

    # Build subgraph over remaining nodes
    sub_adj: Dict[int, List[int]] = {u: [v for v in graph[u] if v in remaining] for u in remaining}
    # Pick an edge to start
    start = None
    for u, vs in sub_adj.items():
        if vs:
            start = u
            break
    if start is None:
        return "Cycle present but no edges in residual graph."

    # Walk to find a cycle (iterative)
    path: List[int] = []
    in_path: Dict[int, int] = {}
    cur = start
    while True:
        path.append(cur)
        in_path[cur] = len(path) - 1
        if not sub_adj[cur]:
            # dead-end; try another node with edges
            next_start = None
            for u, vs in sub_adj.items():
                if vs and u not in in_path:
                    next_start = u
                    break
            if next_start is None:
                return "Could not extract specific cycle path."
            cur = next_start
            continue
        nxt = sub_adj[cur][0]
        if nxt in in_path:
            cycle_nodes = path[in_path[nxt]:] + [nxt]
            break
        cur = nxt

    # Format using evidence
    def cause_name(c: int) -> str:
        return {0: "W→R", 1: "W→W", 2: "R→W"}.get(c, "?")

    lines: List[str] = []
    lines.append(f"Cycle txns: {cycle_nodes}")
    if (len(cycle_nodes) > 10):
        lines[-1] = f"Cycle txns: [{', '.join(map(str, cycle_nodes[:5]))} ... {', '.join(map(str, cycle_nodes[-5:]))}]"
        return "\n".join(lines) # too many nodes, just return the list of nodes

    lines.append("Edges (with example evidence):")
    for i in range(len(cycle_nodes) - 1):
        u = cycle_nodes[i]
        v = cycle_nodes[i + 1]
        evs = evidence.get((u, v), [])
        if not evs:
            lines.append(f"  {u} -> {v} (no evidence captured)")
            continue
        ev = evs[0]
        from_seq, to_seq = ev.get("from_seq", -1), ev.get("to_seq", -1)
        lines.append(
            f"  {u} -> {v} via {cause_name(ev.get('cause', -1))} on key '{ev.get('key','?')}' (from seq {from_seq}, to seq {to_seq})"
        )

    # Add per-transaction ops and cross-tx op order
    tx_set = set(cycle_nodes)
    tx_ops: Dict[int, List[Op]] = defaultdict(list)
    for op in ops:
        if op.txid in tx_set:
            tx_ops[op.txid].append(op)
    # Ensure ordering by seq
    for txid in tx_ops:
        tx_ops[txid].sort(key=lambda o: o.seq)

    lines.append("Transactions ops:")
    for txid in cycle_nodes:
        ops_str = " ".join(f"{o.kind}({o.key})" for o in tx_ops.get(txid, []))
        lines.append(f"  T[{txid}]: {ops_str}")

    lines.append("Operation order for these transactions:")
    filtered = [o for o in ops if o.txid in tx_set]
    filtered.sort(key=lambda o: o.seq)
    order_str = " ".join(f"{o.kind}_{o.txid}({o.key})" for o in filtered)
    lines.append(f"  {order_str}")

    return "\n".join(lines)
#
# ---------- Workloads ----------

def wl_counter(r: ConcurrentRunner, txn: "Transaction") -> None:
    v = r.log_read(txn, "ctr") or 0
    r.log_write(txn, "ctr", v + 1)
    r.log_commit(txn)


def wl_bank(r: ConcurrentRunner, txn: "Transaction") -> None:
    # Initialize lazily if keys are absent
    va = r.log_read(txn, "a") or 0
    vb = r.log_read(txn, "b") or 0
    if va == 0 and vb == 0:
        # Initialize total to 1000 on first txn that finds zeros
        r.log_write(txn, "a", 500)
        r.log_write(txn, "b", 500)
        r.log_commit(txn)
        return
    x = random.randint(1, 5)
    if random.random() < 0.5:
        va, vb = va - x, vb + x
    else:
        va, vb = va + x, vb - x
    r.log_write(txn, "a", va)
    r.log_write(txn, "b", vb)
    r.log_commit(txn)


def wl_deadlock_prone(r: ConcurrentRunner, txn: "Transaction") -> None:
    """Write two hot keys in random opposite orders to provoke potential deadlocks if CC blocks."""
    keys = ("A", "B")
    if random.random() < 0.5:
        first, second = keys
    else:
        first, second = keys[::-1]
    v1 = r.log_read(txn, first) or 0
    v2 = r.log_read(txn, second) or 0
    r.log_write(txn, first, v1 + 1)
    r.log_write(txn, second, v2 + 1)
    r.log_commit(txn)


# ---------- TPC-C Workload ----------

# TPC-C configuration constants
TPCC_NUM_WAREHOUSES = 8
TPCC_DISTRICTS_PER_WH = 10
TPCC_CUSTOMERS_PER_DIST = 100
TPCC_NUM_ITEMS = 1000
TPCC_INITIAL_STOCK = 100
TPCC_INITIAL_BALANCE = 10000
TPCC_INITIAL_YTD = 0


def init_tpcc_data(store: VersionedKVStore) -> None:
    """
    Initialize the TPC-C database with warehouses, districts, customers,
    items, and stock. This should be called before running wl_tpcc.
    
    Key naming conventions:
    - w_{wid}_ytd: Warehouse year-to-date balance
    - d_{wid}_{did}_ytd: District year-to-date balance  
    - d_{wid}_{did}_next_oid: District next order ID counter
    - c_{wid}_{did}_{cid}_bal: Customer balance
    - c_{wid}_{did}_{cid}_ytd: Customer year-to-date payment
    - item_{iid}_price: Item price (cents)
    - s_{wid}_{iid}_qty: Stock quantity for item at warehouse
    """
    writes: Dict[str, int] = {}
    
    # Initialize warehouses
    for wid in range(1, TPCC_NUM_WAREHOUSES + 1):
        writes[f"w_{wid}_ytd"] = TPCC_INITIAL_YTD
        
        # Initialize districts per warehouse
        for did in range(1, TPCC_DISTRICTS_PER_WH + 1):
            writes[f"d_{wid}_{did}_ytd"] = TPCC_INITIAL_YTD
            writes[f"d_{wid}_{did}_next_oid"] = 1  # Start order IDs at 1
            
            # Initialize customers per district
            for cid in range(1, TPCC_CUSTOMERS_PER_DIST + 1):
                writes[f"c_{wid}_{did}_{cid}_bal"] = TPCC_INITIAL_BALANCE
                writes[f"c_{wid}_{did}_{cid}_ytd"] = TPCC_INITIAL_YTD
    
    # Initialize items (global, not per-warehouse)
    for iid in range(1, TPCC_NUM_ITEMS + 1):
        writes[f"item_{iid}_price"] = random.randint(100, 10000)  # Price in cents
    
    # Initialize stock per warehouse per item
    for wid in range(1, TPCC_NUM_WAREHOUSES + 1):
        for iid in range(1, TPCC_NUM_ITEMS + 1):
            writes[f"s_{wid}_{iid}_qty"] = TPCC_INITIAL_STOCK
    
    # Write all initial data at timestamp 0
    store.put(writes, commit_ts=0)


def _tpcc_new_order(r: ConcurrentRunner, txn: "Transaction") -> bool:
    """
    TPC-C New Order transaction.
    
    1. Select a random warehouse, district, and customer
    2. Read district's next order ID and increment it
    3. Read customer data
    4. For 5-15 order lines:
       - Select a random item
       - Read item price
       - Read and decrement stock quantity
    5. Write the order record
    
    Returns True if transaction should commit, False if should abort.
    """
    # Select random warehouse, district, customer
    wid = random.randint(1, TPCC_NUM_WAREHOUSES)
    did = random.randint(1, TPCC_DISTRICTS_PER_WH)
    cid = random.randint(1, TPCC_CUSTOMERS_PER_DIST)
    
    # Read and update district's next order ID
    next_oid_key = f"d_{wid}_{did}_next_oid"
    next_oid = r.log_read(txn, next_oid_key) or 1
    r.log_write(txn, next_oid_key, next_oid + 1)
    oid = next_oid
    
    # Read customer balance (for validation in real TPC-C)
    cust_bal_key = f"c_{wid}_{did}_{cid}_bal"
    r.log_read(txn, cust_bal_key)
    
    # Generate order lines (5-15 items per order as per TPC-C spec)
    num_items = random.randint(5, 15)
    total_amount = 0
    
    for ol_num in range(1, num_items + 1):
        # Select random item
        iid = random.randint(1, TPCC_NUM_ITEMS)
        
        # Read item price
        item_price_key = f"item_{iid}_price"
        price = r.log_read(txn, item_price_key) or 100
        
        # Read and decrement stock
        stock_key = f"s_{wid}_{iid}_qty"
        stock_qty = r.log_read(txn, stock_key) or TPCC_INITIAL_STOCK
        
        # Calculate order quantity (1-10 units)
        ol_qty = random.randint(1, 10)
        
        # Decrement stock (with wrap-around if below 10)
        new_qty = stock_qty - ol_qty
        if new_qty < 10:
            new_qty += 91  # Restock
        r.log_write(txn, stock_key, new_qty)
        
        # Accumulate total
        total_amount += price * ol_qty
    
    # Write order record (total amount)
    order_key = f"o_{wid}_{did}_{oid}_total"
    r.log_write(txn, order_key, total_amount)
    
    return True


def _tpcc_payment(r: ConcurrentRunner, txn: "Transaction") -> bool:
    """
    TPC-C Payment transaction.
    
    1. Select a random warehouse and district
    2. Update warehouse year-to-date balance
    3. Update district year-to-date balance
    4. Select a random customer
    5. Update customer balance (decrease) and year-to-date payment (increase)
    
    Returns True if transaction should commit, False if should abort.
    """
    # Select random warehouse, district, customer
    wid = random.randint(1, TPCC_NUM_WAREHOUSES)
    did = random.randint(1, TPCC_DISTRICTS_PER_WH)
    cid = random.randint(1, TPCC_CUSTOMERS_PER_DIST)
    
    # Payment amount (between $1.00 and $5000.00)
    payment_amount = random.randint(100, 500000)
    
    # Update warehouse YTD
    wh_ytd_key = f"w_{wid}_ytd"
    wh_ytd = r.log_read(txn, wh_ytd_key) or 0
    r.log_write(txn, wh_ytd_key, wh_ytd + payment_amount)
    
    # Update district YTD
    dist_ytd_key = f"d_{wid}_{did}_ytd"
    dist_ytd = r.log_read(txn, dist_ytd_key) or 0
    r.log_write(txn, dist_ytd_key, dist_ytd + payment_amount)
    
    # Update customer balance and YTD payment
    cust_bal_key = f"c_{wid}_{did}_{cid}_bal"
    cust_ytd_key = f"c_{wid}_{did}_{cid}_ytd"
    
    cust_bal = r.log_read(txn, cust_bal_key) or TPCC_INITIAL_BALANCE
    cust_ytd = r.log_read(txn, cust_ytd_key) or 0
    
    r.log_write(txn, cust_bal_key, cust_bal - payment_amount)
    r.log_write(txn, cust_ytd_key, cust_ytd + payment_amount)
    
    return True


def wl_tpcc(r: ConcurrentRunner, txn: "Transaction") -> None:
    """
    TPC-C workload that randomly executes either a New Order or Payment transaction.
    
    Per TPC-C specification, the mix is approximately:
    - 45% New Order
    - 43% Payment
    - (The remaining 12% would be Order-Status, Delivery, Stock-Level which are omitted)
    
    For this simplified benchmark, we use ~50% New Order, ~50% Payment.
    """
    # Randomly select transaction type (roughly 50/50 split)
    if random.random() < 0.5:
        success = _tpcc_new_order(r, txn)
    else:
        success = _tpcc_payment(r, txn)
    
    if success:
        r.log_commit(txn)
    else:
        r.log_rollback(txn)


import runpy
from types import SimpleNamespace

# ---------- Program loading helpers ----------

def _load_program(program_path: str):
    """Dynamically load the candidate program as a simple namespace.

    We use runpy.run_path to execute the file in an isolated namespace and then
    wrap that namespace in a SimpleNamespace so the rest of the evaluator can
    access attributes like `build_cc` as if it were a normal module.
    """
    try:
        ns: Dict[str, Any] = runpy.run_path(program_path)
        module = SimpleNamespace(**ns)
        logger.info("Loaded program from %s with keys: %s", program_path, list(ns.keys()))
        return module
    except Exception as e:
        logger.error("Failed to load program %s: %s", program_path, e)
        raise


def _run_scenario(
    cc_factory,
    workload,
    num_workers: int,
    duration_s: float,
    stall_threshold_s: float,
    store_initializer=None,
) -> Dict[str, Any]:
    """Run one workload scenario and summarize its behavior.
    
    Args:
        cc_factory: Factory function that returns a CC instance.
        workload: The workload function to run.
        num_workers: Number of concurrent worker threads.
        duration_s: Duration to run the workload in seconds.
        stall_threshold_s: Time threshold to detect stalled threads.
        store_initializer: Optional callable to initialize store data before running.
    """
    runner = ConcurrentRunner(cc_factory, num_workers=num_workers, duration_s=duration_s, stall_threshold_s=stall_threshold_s)
    if store_initializer is not None:
        runner.init_store(store_initializer)
    ops, committed, stalled = runner.run(workload)
    graph, evidence = build_precedence_graph_fast(ops)
    has_cycle = has_cycle_kahn(graph)

    scenario = {
        "committed": len(committed),
        "stalled_threads": len(stalled),
        "has_cycle": has_cycle,
        "ops": len(ops),
        "throughput": len(committed) / duration_s if duration_s > 0 else 0.0,
    }

    if has_cycle:
        scenario["cycle_explanation"] = explain_cycle_kahn(graph, evidence, ops)

    return scenario


def evaluate(program_path: str) -> EvaluationResult:
    """
    Entry point used by OpenEvolve to evaluate a concurrency control implementation.

    The returned EvaluationResult includes:
      - correctness: fraction of scenarios that are conflict-serializable and stall-free
      - throughput: average committed transactions per second across scenarios
      - combined_score: weighted combination favoring correctness
    """
    try:
        module = _load_program(program_path)
        if not hasattr(module, "build_cc") or not callable(getattr(module, "build_cc")):
            raise AttributeError("Program must define a callable 'build_cc()' that returns a GeneratedCC instance.")
        
        def cc_factory():
            # The user program is expected to have build_cc() that returns the
            # GeneratedCC concurrency control implementation.
            return module.build_cc()

        # Run four complementary scenarios
        scenarios = {
            # "counters": _run_scenario(cc_factory, wl_counter, num_workers=8, duration_s=1.0, stall_threshold_s=0.5),
            # "bank": _run_scenario(cc_factory, wl_bank, num_workers=8, duration_s=1.0, stall_threshold_s=0.5),
            # "deadlock": _run_scenario(cc_factory, wl_deadlock_prone, num_workers=8, duration_s=5.0, stall_threshold_s=0.2),
            "tpcc": _run_scenario(
                cc_factory, wl_tpcc, 
                num_workers=16, duration_s=30.0, stall_threshold_s=0.5,
                store_initializer=init_tpcc_data
            ),
        }

        total_scenarios = len(scenarios)
        correct = 0
        total_throughput = 0.0

        tpcc_coefficient = 10
        other_coefficient = 1
        weighted_scenarios = 0

        for name, s in scenarios.items():
            serializable = not s["has_cycle"]
            stall_free = s["stalled_threads"] == 0
            is_ok = serializable and stall_free



            if is_ok:
                correct += 1
            
            if name == "tpcc":
                throughput_score += s["throughput"] * tpcc_coefficient
                weighted_scenarios += tpcc_coefficient
            else:
                throughput_score += s["throughput"] * other_coefficient
                weighted_scenarios += other_coefficient

        

        correctness = correct / total_scenarios if total_scenarios > 0 else 0.0
        avg_throughput = throughput_score / weighted_scenarios if weighted_scenarios > 0 else 0.0

        # Map throughput to a bounded score
        throughput_score = avg_throughput / (100000.0 + avg_throughput)

        combined_score = 0.5 * correctness + 0.5 * throughput_score

        metrics = {
            "correctness": float(correctness),
            "throughput": float(avg_throughput),
            "combined_score": float(combined_score),
        }

        artifacts: Dict[str, Any] = {
            "scenario_summaries": scenarios,
        }

        return EvaluationResult(metrics=metrics, artifacts=artifacts)

    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        return EvaluationResult(
            metrics={
                "correctness": 0.0,
                "throughput": 0.0,
                "combined_score": 0.0,
            },
            artifacts={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "full_traceback": traceback.format_exc(),
            },
        )


if __name__ == "__main__":
    # Simple sanity check when running this file directly.
    import os

    here = os.path.dirname(os.path.abspath(__file__))
    program_path = os.path.join(here, "initial_program.py")
    print(evaluate(program_path))