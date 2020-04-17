# dfuse Search
[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/dfuse-io/search)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The dfuse Search engine is an innovative, both historical and real-time,
fork-aware, blockchain search engine.
It is part of **[dfuse](https://github.com/dfuse-io/dfuse)**.


## Features

It can act as a distributed system, composed of real-time and archive
backends, plus a router addressing the right backends, discovered
through an `etcd` cluster.

It supports massively parallelized indexing of the chain (put in the
power, and process 20TB of data in 30 minutes).  It is designed for
high availability, and scales horizontally.

It feeds from a _dfuse source_, like [dfuse for EOSIO](https://github.com/dfuse-io/dfuse-eosio)


## Installation & Usage

See the different protocol-specific `dfuse` binaries at https://github.com/dfuse-io/dfuse#protocols

Current `search` implementations:

* [**dfuse for EOSIO**](https://github.com/dfuse-io/dfuse-eosio)
* **dfuse for Ethereum**, soon to be open sourced


## Contributing

**Issues and PR in this repo related strictly to the core search engine.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

This codebase uses unit tests extensively, please write and run tests.


## License

[Apache 2.0](LICENSE)
