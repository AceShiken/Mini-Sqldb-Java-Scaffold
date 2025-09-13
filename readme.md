# MiniSQLDB – Disk‑backed Heap + INSERT + SELECT (v0.3)


A runnable Maven scaffold that **writes rows to disk** (no in‑memory only cache) using a simple heap table file format. Supports:
- `CREATE TABLE name (col TYPE, ...)` (types: `INT`, `VARCHAR`)
- `INSERT INTO name (col, ...) VALUES (value, ...)`
- `.tables` and `.dump <table>` in the REPL


Records are stored page‑locally as `[int rowLen][rowBytes...]`, and `rowBytes` encodes each column (`INT` = 4 bytes, `VARCHAR` = `[int len][bytes]`). Pages track a 4‑byte **used** pointer.

---

### Notes & invariants
- Page header: first 4 bytes = **used** pointer; new pages initialize to 4.
- Each record stored as `[int rowLen][rowBytes]`; `rowBytes` is the RowFormat payload (schema aware).
- This is append-only. No deletes/updates yet. Checkpointing is a no-op aside from WAL sync.
- Next steps: WHERE filter on scan, WAL redo records around page writes, simple index (B+Tree) for point lookups.

---

This is a runnable Maven project scaffold you can paste into a folder.
File paths are shown as headers.
Build with `mvn -q -DskipTests package` and
run with `java -jar target/minisqldb-0.2.0.jar --data ./data`.

---

### Next Steps (in code comments)
- Implement a heap table file format and `INSERT` execution.
- Add a simple row serializer for INT/VARCHAR.
- Write WAL records around page writes, and replay on startup.
- Build a tiny B+Tree for secondary index.
- Replace the toy page cache with an LRU.

---

### Usage
```
db> CREATE TABLE users (id INT, name VARCHAR);
OK: created table users


db> INSERT INTO users (id, name) VALUES (1, 'Alice');
OK: 1 row inserted


db> INSERT INTO users (id, name) VALUES (2, 'Bob');
OK: 1 row inserted


db> .dump users
{id=1, name=Alice}
{id=2, name=Bob}

db> .truncate users
OK: truncated

db> .drop users
OK: dropped

db> CREATE TABLE users (id INT, name VARCHAR);
OK: created table users
```

---


Now supports:
- `CREATE TABLE` (INT, VARCHAR)
- `INSERT INTO ...`
- `SELECT * FROM table` and `SELECT * FROM table WHERE col = value`
- `.tables` and `.dump <table>`

---
