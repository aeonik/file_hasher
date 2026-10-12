# Changelog

Notable changes to this project are documented here. The format follows
[keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### Added

- XTDB 2.1 inventory scanner under `aeonik.inventory.*`, with `status` and
  `scan` commands.
- Architecture notes in `doc/architecture.md`.

### Changed

- `clojure -M:run-m` starts the inventory CLI. The earlier PostgreSQL manifest
  tool remains available as `clojure -M:legacy`.

[Unreleased]: https://github.com/aeonik/file_hasher/compare/v0.0.1-alpha...HEAD
