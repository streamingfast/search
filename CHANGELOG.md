# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.0.1] 2020-06-22

### Changed
* add `shutdown-delay` flag to live and archive, now 1sec by default (instead of 5 and 7 seconds previously)
* `--listen-grpc-addr` now is `--grpc-listen-addr`

### Fixed
* Indexer no longer overflows on negative startblocks on new chains, it fails fast instead.
* Fixed relative-start-block truncation in search-archive
* Fixed forkresolver nil pointer (app was previously 100% broken)

### Changed
* roarCache is now stored and looked up based on a NORMALIZED version of the query string. (ex: `a:foo b:bar` is now equivalent to `b:bar a:foo`, etc.)

## 2020-03-21

### Changed

* License changed to Apache 2.0
