# file_hasher

A filesystem-neutral, temporal file inventory for answering recovery questions:

- What did a path contain during a particular scan?
- When was a content hash last observed?
- Which source content has no observation on an independent backup root?
- Did a path disappear, change, or probably move?
- Was a scan complete, interrupted, or affected by files changing during hashing?

The current implementation targets XTDB 2.1 and keeps the original
PostgreSQL/manifest recovery prototype under the existing `aeonik.file-hasher`
namespace. The new implementation lives under `aeonik.inventory.*`.

## Design

The scanner does not require Btrfs or any other particular filesystem.
Snapshotting is an optional consistency adapter to add outside the portable
scanner. By default a configured root is traversed across nested filesystem
boundaries without following symbolic links. Pass `--same-filesystem` when
the desired policy is to stop at those boundaries.

Each scan is stored as `running` before observations are written in bounded
batches. It is changed to `complete` only after traversal and all writes
succeed. Files are statted before and after hashing; a changed file is retained
as an `unstable` observation rather than silently treated as a trustworthy
snapshot.

See [doc/architecture.md](doc/architecture.md) for the data model and
consistency rules.

## Requirements

- JDK 21
- Clojure CLI
- XTDB 2.1 reachable through its PostgreSQL-wire endpoint

XTDB must be configured with persistent log and storage paths. Its in-memory
defaults are appropriate only for tests.

XTDB 2.1's in-process node is tested here with JDK 21. This workstation's
default JDK 26 no longer contains the preview structured-concurrency class
XTDB 2.1 references, so select the installed JDK 21 for development and tests.

## Usage

Check connectivity:

```shell
clojure -M:run-m status
```

Scan a small explicit root:

```shell
clojure -M:run-m scan --root /path/to/root --root-id workstation-documents
```

Connection settings default to localhost and can be supplied without storing
credentials in the repository:

```text
FILE_HASHER_XTDB_HOST
FILE_HASHER_XTDB_PORT
FILE_HASHER_XTDB_DATABASE
FILE_HASHER_XTDB_USER
FILE_HASHER_XTDB_PASSWORD
```

The scanner currently uses SHA-256. Do not point it at a large backup root
until a small scan and database backup/restore have been verified.

## Tests

```shell
env JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
  PATH=/usr/lib/jvm/java-21-openjdk/bin:$PATH \
  clojure -M:test
```

Tests start an ephemeral in-process XTDB node and scan only temporary fixtures.

## Legacy recovery tool

The historical entry point is still available for reference:

```shell
clojure -M:legacy
```

It expects the original ignored `resources/secrets.edn` and recovery
manifests; it is not part of the new scanner architecture.
