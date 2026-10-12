# file_hasher

A filesystem-neutral, temporal file inventory. Each scan records what a root
contained, including content hashes, so these recovery questions can be
answered from the stored observations:

- What did a path contain during a particular scan?
- When was a content hash last observed?
- Which source content has no observation on an independent backup root?
- Did a path disappear, change, or probably move?
- Was a scan complete, interrupted, or affected by files changing during hashing?

The CLI currently checks XTDB connectivity and runs scans. It does not yet
run those recovery queries. When querying XTDB directly, ignore scans whose
status is not `complete`.

The current implementation targets XTDB 2.1. The original PostgreSQL manifest
recovery tool remains under `aeonik.file-hasher`. The scanner lives under
`aeonik.inventory.*`.

## Design

The scanner does not require Btrfs or any other particular filesystem.
Snapshotting is an optional consistency adapter to add outside the portable
scanner. By default a configured root is traversed across nested filesystem
boundaries without following symbolic links. Pass `--same-filesystem` when
the desired policy is to stop at those boundaries.

Each scan is stored as `running` before observations are written in bounded
batches. It is changed to `complete` only after traversal and all writes
succeed. A failure leaves the scan as `failed`. Files are statted before and
after hashing; a changed file is retained as an `unstable` observation rather
than silently treated as a trustworthy snapshot. A nested mount that the scan
is configured not to cross is stored as `mount_boundary`.

See [doc/architecture.md](doc/architecture.md) for the data model and
consistency rules.

## Requirements

- JDK 21 (see `.java-version`)
- Clojure CLI
- XTDB 2.1 reachable through its PostgreSQL-wire endpoint

XTDB must be configured with persistent log and storage paths. Its in-memory
defaults are appropriate only for tests.

Use JDK 21 to run XTDB 2.1. This version references the preview
structured-concurrency class from that release. Newer JDKs, including JDK 26,
no longer provide that class, so the in-process node used by the tests will
not start on them.

## Usage

Check connectivity:

```shell
clojure -M:run-m status
```

Scan one explicit root:

```shell
clojure -M:run-m scan --root /path/to/root --root-id workstation-documents
```

Show the scan flags:

```shell
clojure -M:run-m scan --help
```

### `status`

Prints XTDB node status for the configured connection.

### `scan`

Walks `--root` and writes the scan plus observations to XTDB.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--root PATH` | required | Filesystem root to scan |
| `--root-id ID` | derived from host and absolute path | Stable name for this root |
| `--same-filesystem` | off | Stop at nested filesystem boundaries |
| `--batch-size N` | `1000` | Observations per XTDB transaction |
| `--host HOST` | `127.0.0.1` | XTDB host |
| `--port PORT` | `5432` | XTDB PostgreSQL-wire port |
| `--database DATABASE` | `xtdb` | XTDB database name |
| `--user USER` | unset | XTDB user |
| `-h`, `--help` | | Show help |

`status` accepts the same connection flags. The password is not a flag. Set
it with the environment variable below, or omit it when the node has no
authentication.

```text
FILE_HASHER_XTDB_HOST
FILE_HASHER_XTDB_PORT
FILE_HASHER_XTDB_DATABASE
FILE_HASHER_XTDB_USER
FILE_HASHER_XTDB_PASSWORD
```

The scanner hashes regular files with SHA-256. Do not point it at a large
backup root until a small scan and a database backup/restore have been
verified.

## Tests

```shell
env JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
  PATH=/usr/lib/jvm/java-21-openjdk/bin:$PATH \
  clojure -M:test
```

On a machine where `java` is already JDK 21, `clojure -M:test` is enough.
Tests start an ephemeral in-process XTDB node and scan only temporary fixtures.

## Legacy recovery tool

```shell
clojure -M:legacy
```

That alias runs `aeonik.file-hasher`. In `deps.edn` it also passes
`--directory` pointed at a local backup mount; change that path before using
the alias on another machine. The tool reads the ignored
`resources/secrets.edn` and the original hash-list manifests. It is not part
of the scanner.
