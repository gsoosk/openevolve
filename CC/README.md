## HakiCC: Concurrency Control Example

This example demonstrates how OpenEvolve can evolve **concurrency control (CC)
algorithms** for an in-memory versioned key–value store, starting from a simple
baseline implementation.

### Problem Description

We model a transactional storage system with:

- A versioned key–value store (`VersionedKVStore`) that supports:
  - `get(key: str, ts: int) -> Optional[Any]`
  - `put(writes: Dict[str, Any], commit_ts: int) -> None`
- A transaction abstraction (`Transaction`) with:
  - `txid`, `start_ts`, `status` (`TxStatus.ACTIVE/COMMITTED/ABORTED`)
  - `read_set`, `write_set`, and free-form `meta` for CC-specific metadata.
- A concurrency-control plugin interface (`ConcurrencyControl`) with methods:
  - `begin(store) -> Transaction`
  - `read(txn, key, store) -> Optional[Any]`
  - `write(txn, key, value, store) -> None`
  - `commit(txn, store) -> CommitResult`
  - `rollback(txn, store) -> None`

The CC algorithm is responsible for:

- Ensuring that the history of **committed** transactions is
  **conflict-serializable**.
- Avoiding pathological stalls or deadlocks under a deadlock-prone workload.
- Achieving good throughput under concurrent workloads.

### Code Structure

- `initial_program.py`
  - Defines core types: `TxStatus`, `Transaction`, `CommitResult`,
    `ConcurrencyControl`.
  - Contains an `EVOLVE-BLOCK` with a baseline `GlobalLockCC` implementation
    that serializes all transactions using a single global lock (simple but
    correct).
  - Exposes a factory function:
    - `build_cc() -> ConcurrencyControl`
    - The evaluator calls this to obtain the CC implementation under test.

- `evaluator.py`
  - Loads the candidate program from `initial_program.py`.
  - Discovers the CC implementation via:
    - `build_cc()`, if present, or
    - A concrete subclass of `ConcurrencyControl` defined in the module.
  - Wraps `VersionedKVStore` in `MockKVStore` to log actual storage `get/put`
    operations along with transaction IDs.
  - Runs several **concurrent workloads** using `ConcurrentRunner`:
    - `wl_counter`: increments a single counter key.
    - `wl_bank`: transfers funds between two accounts while preserving total.
    - `wl_deadlock_prone`: writes two hot keys in opposite orders to provoke
      potential deadlocks if CC blocks incorrectly.
  - Builds a **precedence graph** over committed transactions and checks:
    - Whether there is any cycle (non-serializable history).
    - Whether any worker threads stall under the deadlock-prone workload.
  - Returns an `EvaluationResult` with:
    - `correctness`: fraction of scenarios that are serializable and, for the
      deadlock workload, also stall-free.
    - `throughput`: average committed transactions per second across scenarios.
    - `combined_score`: a weighted combination that heavily favors correctness.

- `config.yaml`
  - Configures the LLM, evaluator, and database/evolution settings for this
    example.
  - The `system_message` instructs the model to implement a CC algorithm by
    modifying only the `EVOLVE-BLOCK` in `initial_program.py`, using only the
    provided interfaces.

### Running the Example

From the repository root:

```bash
cd examples/HakiCC
python ../../openevolve-run.py initial_program.py evaluator.py --config config.yaml
```

This will:

- Use `GlobalLockCC` as the initial baseline implementation.
- Run evolutionary search to propose new versions of the CC algorithm inside
  the `EVOLVE-BLOCK`.
- Evaluate each candidate using the heavy concurrent workloads in
  `evaluator.py` and score it using `correctness`, `throughput`, and
  `combined_score`.

### Adapting the CC Algorithm

To experiment manually with different concurrency control schemes:

1. Edit the class inside the `EVOLVE-BLOCK` in `initial_program.py`
   (for example, replacing `GlobalLockCC` with a strict 2PL, OCC, or hybrid
   algorithm).
2. Keep the public interface of `ConcurrencyControl` and `build_cc()` intact.
3. Use `Transaction.meta` and internal fields of your CC class to maintain
   any lock tables, timestamp allocators, or wait-for graphs you need.

As long as the interface is respected and you only use `VersionedKVStore.get`
and `VersionedKVStore.put` to touch storage, the existing evaluator and config
will continue to work without modification.


