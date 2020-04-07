Search architecture history
---------------------------

* First version of Search
  * Single process
  * Building indexes each 5000 blocks
  * Uploaded to Google Storage in case we lost the underlying disk
  * Indexes real-time blocks
  * Handled reversible segments of blocks, and fork navigation.

* Second version
  * Two processes: archive and live router.
  * Archive:
    * Queried less often than the reversible segment (head and
      real-time things of the chain)
    * Required a persistent disk, with huge amounts of indexes (20k
      indexes, 7-8TB) and looots of RAM (600GB)
    * Different operational requirements than the live router (K8s statefulset)
  * Live router:
    * Handled all real-time queries
    * Delegated to Archive for block segments that are set in stone.
    * Lightweight indexing (1 blocks at a time), all in RAM.
    * Smaller operational requirements, easy to scale out (K8s deployment)

* v2 Search2 version 2
  * 3 processes, synchronized through discovery service (dmesh/etcd)
  * `router` which ONLY routes queries to the different backend
    * Discovers the existence, the block ranges, the features
      available on other backends (ex: `live` serves `reversible`
      segments, whereas `archive` does not)
    * Spreads the queries to the different backends depending on
      ranges of blocks coming from users.
  * `archive backend`
    * We can now have multiple tiers of archives, all blended together.
    * We have 5000 blocks indexes for the 0-87M blocks (still fast
      with the density of the chain at that point).
    * We have 500 blocks indexes for 87M-HEAD, which are a lot denser
      thanks to EIDOS mining.
    * We also have 50 blocks indexes, that will merely try to overlap
      with the previous tiers (of 500 and 5000)
      * Main reason: whenever there is downtime, we can do parallel
        reprocessing and cover all block ranges: 50 blocks can be
        extremely parallelized, allows us to be back up in minutes
        rather than hours.
  * `live backend`
    * Only serves 1-block indexes, in addition to reversible
      segments + fork navigation.
    * Keeps its memory use contained because it now watches the
      archive backends:
      * If there is sufficient coverage of certain block ranges by
        archive nodes, then the `live backends` can truncate the
        blocks they're holding.
    * Can scale out maximally, with low effect
      * In version 1, scaling out that Search tier would have required
        7TB of disk space (!)
