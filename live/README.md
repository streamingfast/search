CLI call
--------

    echo '{"lowBlockNum": 40000000, "highBlockNum": 41000000, "descending": true, "limit": 100, "query": "status:executed"}' | grpcurl -plaintext -d @ localhost:50002 search.Archive.StreamMatches
    echo '{"lowBlockNum": 40000000, "highBlockNum": 41000000, "descending": true, "limit": 100, "query": "status:executed"}' | grpcurl -plaintext -d @ localhost:50001 search.Router.StreamMatches

gRPC UI
-------

    grpcui -port 60002 -plaintext localhost:50002


Planned renames
---------------

* shardIndex:
  * startBlock -> lowBlock
  * endBlock -> highBlock

* queries also:
  * lowBlockNum
  * highBlockNum

or should it be `hi` and `lo` ?
  `loBlockNum`, `hiBlockNum`


Case where we should fail
-------------------------

* Going backwards, if you're giving me a forked head block, I fail.

  The backwards cursor should pass the head block in there, even as we
  navigate towards the beginning of the chain.
