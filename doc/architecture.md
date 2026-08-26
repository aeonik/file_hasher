# Architecture

## Boundaries

`aeonik.inventory.scanner` knows about files and emits immutable observation
documents. It has no database dependency.

`aeonik.inventory.store` defines the lifecycle required of a store:

1. Record a running scan.
2. Persist bounded batches of observations.
3. Mark the scan complete, or failed, with final counts.

`aeonik.inventory.xtdb` implements that lifecycle with XTDB 2. A future
exporter can implement the same protocol for portable manifests without
changing traversal or hashing.

## Identity

- A scan receives a random UUID.
- A root ID is supplied by configuration or deterministically derived from
  host name and normalized absolute path.
- An observation ID is derived from scan ID plus relative path.
- A content ID is derived from hash algorithm, digest, and size.

A path, inode, or content hash is deliberately not treated as a persistent
file identity. An observation is what a particular completed scan witnessed
at a particular root and relative path.

## Time

XTDB system time records when facts entered the inventory. Domain timestamps
record when scanning and individual observations happened. We do not yet map
observation time onto XTDB valid time because a scan only proves state at an
observation point; it does not prove the exact interval during which a file
state was valid.

This leaves room for later importing older manifests: their historical
observation timestamps can be preserved without pretending they entered the
database earlier.

## Consistency

- Symbolic links are recorded but never followed.
- Nested mounts are traversed by default. `same-filesystem` is an explicit
  policy, never an implicit assumption about subvolumes.
- Regular files are statted before and after hashing. Size, modification time,
  and provider file key must remain equal for status `stable`.
- Read failures and traversal failures are stored as observations.
- Observation writes are chunked, but only a final `complete` scan record
  claims coverage. Queries comparing roots must ignore running and failed
  scans.

A filesystem-specific snapshot adapter can provide a stable scan root for
Btrfs, ZFS, LVM, APFS, or VSS. The portable scanner does not require one.

## Storage and recovery

XTDB is the query system, not the only recovery artifact. Completed scans
should eventually be exported to a checksummed, portable format and replicated
outside both the inventoried root and XTDB's own failure domain.

The XTDB transaction log and indexed storage must both be persistent and
backed up coherently. Default in-memory XTDB configuration is test-only.
