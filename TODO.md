# Design review notes

Improvements identified in a design review (2026-07-03), to be worked through
over time. Connection config stays env-var based by design: the extension runs
on cluster nodes where `HADOOP_CONF_DIR` / `HADOOP_USER_NAME` are already set
for Hive/Spark/the HDFS CLI, so it "just works" — no DuckDB secrets
integration.

## 1. Generate `hdfs_bridge.h` with cbindgen

Three definitions are hand-mirrored between `src/include/hdfs_bridge.h` and
`hdfs-bridge/src/lib.rs`, guarded only by "keep in sync" comments:
`hdfs_error_code_t` / the Rust constants, `hdfs_status_t` / `Status`, and
`hdfs_dir_entry_t` / `DirEntry`. Generating the header from `lib.rs` with
cbindgen (Corrosion can run it as a build step) turns a drifted struct into a
build failure instead of memory corruption. cbindgen carries Rust doc comments
through, so the header docs survive.

## 2. Unify `ListFilesExtended` onto the streaming listing

`ListFilesExtended` (`src/hdfs_filesystem.cpp`) is the one listing path still
using the materializing `hdfs_bridge_list_status`; everything else streams.
Its callback interface is already incremental, so it can be driven from an
`OpenListStream`, after which `hdfs_bridge_list_status` (and the recursive
mode of `client.list_status`) can be deleted from the ABI. Likely leftover
from before the streaming API existed.

## 3. Make the file-handle → client lifetime explicit

An `HdfsFileHandle` holds a raw `hdfs_reader_t*` but not the
`shared_ptr<hdfs_client_t>` it was created from. If a connection-level retry
invalidates the client while a read handle is still open, the client can be
freed out from under the reader. This is safe *today* because hdfs-native's
`FileReader` is self-contained after open (it has its block locations and
talks to DataNodes directly; it never goes back through the `Client`) — a
fact that is invisible from the C++ side, and an upstream hdfs-native change
(e.g. a `FileReader` that lazily re-fetches block locations through the
client) would break it silently. Fix: store the `shared_ptr<hdfs_client_t>` in
`HdfsFileHandle` so the handle keeps its client alive — one refcount, makes
ownership self-evident. At minimum, document the invariant next to the
existing `Send`/`Sync` static asserts in `client.rs`.

## Smaller items

- **Failover retry is single-shot and immediate** *(not yet discussed —
  keep/drop?)*: `Execute()` retries once, immediately, on
  `HDFS_ERR_CONNECTION`. An HA failover takes seconds, so the reconnect often
  hits the standby again and the query dies anyway. A small bounded backoff
  (e.g. 3 attempts, exponential from ~100ms) would ride out failovers; keep
  the budget small so genuine "cluster is down" errors aren't delayed much.
  Same shape applies to the stream-side retry in `HdfsListStream::Next`.
- **Double-path error messages**: the bridge prefixes errors with the path
  (`"stat '/a/b' failed: ..."`) and the C++ wraps again with the URL
  (`"Failed to stat HDFS file 'hdfs://...': ..."`), so users see the path
  twice. Pick one layer to own context — the C++ side has the full URL, so
  strip the path from the bridge messages.
- **`Seek()` on a write handle** silently moves `position` even though writes
  are append-only, making `SeekPosition` lie. `Seek` past EOF on a reader
  isn't validated until the read. Cheap guards in `HdfsFileSystem::Seek`.
- **Raw `HdfsFileSystem*` in `function_info`**: works because the
  VirtualFileSystem owns the filesystem for the DB lifetime, but resolving it
  from `ClientContext` at bind time would remove the dangling-pointer
  reasoning and the `hdfs_ptr` handoff in `LoadInternal`.
- **`hdfs_exists` issues one stat RPC per row** with no dedup or caching;
  `SELECT hdfs_exists(path) FROM big_table` will crawl. Probably just needs a
  README note (alongside the existing "Keeping it fast" section).
- **Test asymmetry**: `glob.rs` has good unit tests, but the C++ layer
  (`ParseHdfsPath`, the `Execute` retry state machine, `BridgeStatus`,
  glob-vs-list dispatch in `hdfs_ls`) is only exercised via the Docker-gated
  integration test, and much of it is pure logic testable without a cluster.
  hdfs-native's `minidfs` harness could also give the Rust client layer
  cluster-backed tests in plain `cargo test`.
