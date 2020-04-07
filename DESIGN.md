Splitting the indexing job from serving job
-------------------------------------------

* The `Indexer` is a new object that should abstract the indexing part
  from the Serving part (or `ArchiveBackend`). It is specific to the
  `archive` package.

  * The interaction between the `Indexer` and the `IndexPool` should
    happen only through Google Storage.

    * The `Indexer` producing the next meaningful index, and
      interrupting its work when the IndexPool suddenly has the
      currently-worked-on index loaded.

    * The `IndexPool` watches Google Storage, and loads the next
      available index the moment it's available, and signals to the
      Indexer that it loaded it.  The Indexer can then stop its job

    * The `IndexPool` is currently aware of the `SearchPeer` to mark it
      as `ready`.

      * It is also accessed via
        `pipeline.IndexPool.SearchPeer`.. maybe the pipeline can have
        its own reference, or have simpler "update funcs", detached
        from the SearchPeer itself. Easier for testing also.

* Split concerns between `IndexPool` and `ArchiveBackend`, more
  cleanly.

* Right now, we gate the DOWNLOADING of indexes based on the
  startBlock, but we don't gate the OPENING of the indexes based on
  that number.

  * Upon boot, with a `--start-block` for the Index, we want to DELETE
    the on-disk indexes.. AND NOT load them.


Contract between Router and Backend
-----------------------------------

The Router takes a request with potentially negative block numbers
offsets.

It forwards the requests to Backends, with those numbers resolved to
absolute numbers.

The backends do NOT handle the limit, they do NOT manage the cursor:
the router does those two things. When the limit is reached, the
Router cancels the incoming stream of its backends if any are still
running.

The Router takes the `last-block-read` from each Backend request, and
uses that (+1 or -1 depending on sort direction) to query the next
backend or segment.

It is POSSIBLE that a backend sends a HIGHER `last-block-read` than
the `HighBlockNum` provided, but only in the case where there were no
results in the index.  If there were results in the index past the
`HighBlockNum`, then `last-block-read` would be truncated to the exact
`HighBlockNum` and the results past that block would not be sent as
results.  The `Router`, therefore, can trust that `last-block-read`
was indeed processed, and can move on to the next shard if necessary,
or simply say that `range-completed: true` if that `last-block-read >=
HighBlockNum` (larger than is important in the condition).

The `Router` needs to watch if there is a `block_id` in the cursor,
and ONLY forward those requests to a peer that `ServesReversible`.  If
the range if out of bound, then the cursor is _dead_ (until we
implement the large paragraph).

The `BackendRequest` Bounds
-----------------------------------

The Router will take the request with potentially relative block numbers
(negative numbers) and will forward it to the Backend with absolute numbers


if the request is `ascending` and the targeted backend serves __reversible__
blocks (a.k.a a live backend), the `highBlockNum` of the `BackendReqest` will
be the requested `queryHighBlockNum`. Since the live backend will attempt to resolve
the query until that desired `highBlockNum`.

If the targeted backend server __irreversible__ blocks (a.k.a an archive backend), the
`highBlockNum` will be the the smallest value between the targeted backend's `virtualHead` and
the query's `highBlockNum`




The `range-completed` trailer
-----------------------------------

`eosws` reads the `range-completed` in order to return a cursor or
NOT.  If the range was completely searched, then the last result will
have no cursor.

Therefore, the `Router` should use the `last-block-read` from its backing
services, to compute the `range-completed` value.

If the Router interrupts because of a limit, it therefore knows it has
NOT completed the range (or cannot guarantee it anyways), so it
returns `range-completed: false` in that case.

The backends do NOT need to send `range-completed` trailers.


`dgraphql`'s role, regarding cursor
-----------------------------------

TODO: If we receive a cursor that contains a `block_id`, we assume
that we provided this cursor while it was not certain that this block
was going to stick.  `dgraphql` could validate that the `block_id`
passed irreversibility, and if so, transform the cursor into an
"irreversible" cursor.. This would avoid having to navigate any forks.
If the `block_id` passed irreversibility but is stale, then we need
(fingers crossed) need to navigate our user out of this fork. This
will only work right now, if the block is still in the live backend's
memory.

This scenario needs to be tested thoroughly


Rework and merge of live search and previous search
---------------------------------------------------

* Always search ascending in indexes
* Always search ALL the index, and do the truncation for limit more upstream (by the caller).
* Fork signal: make it disappear. No checks, new cursor method.

* Have ONE way to search the index, the output can be taken by all three outputs:
  * Receive the index as a param, do the search on it, return the matches in one blob
  * The caller can navigate it in whatever direction he wants,
    * Apply any gating he wants.
  * Then:
    * Streams it out, or:
    * Accumulate it into a JSON response for REST


* REST for /v0/search/transactions in `eosws`
  * Call `LiveRouter` in the right direction, and fetch from EOSDB the Lifecycle
    * Until we rework it to be simply a GraphQL call (ditch the lifecycle, make it only a trx),
      so `eosws` becomes an empty can.

* GraphQL Query (non-streaming) in `grapheos` to do PAGED search:
  * Calls `LiveRouter`, streams `TrxMatch`, fetches missing payloads from EOSDB,
    * Interrupt the stream when its limit has been reached.

* GraphQL Subscription (streaming) in `grapheos`:
  * Calls `LiveRouter`, streams `TrxMatch`, fetches missing payloads from EOSDB
    * Streams out the results.


--------------

Search now should ONLY stream out `TrxMatch`, blindfolded, until it is stopped.
* Returns payload when available from live.


The `shardIndex` and `SimpleIndex` objects need an abstraction:
 * remove `blockID` from the `shardIndex`
 * only a getter function for the actual bleve Index

--------------

Stack:
 * co-routines should turn down each other
 * move the cursor logic
   *

---------------

Call hierarchy:
* End-user calls: Archive.StreamTrx
  * Parse query, adjust blocks range
  * Run the Archive query
    * In parallel, run the individual index query
    * Stream back
  * Consume the TrxMatch chan, wait for the Cursor gate, funnel them back to the calling `stream.Send()`.

* End-user calls: Router.StreamTrx
  * Checks the different segments that need to be called, in the right order
    * runFixFork
    * runArchiveSearch
    * runRealtime
  * When calling the `runRealtime`, we do:




Error handling
--------------

Improve on gRPC -> HTTP error handling.
See:
* https://jbrandhorst.com/post/grpc-errors/
* https://godoc.org/google.golang.org/genproto/googleapis/rpc/errdetails
  (and its proto equiv: https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto)
* https://developers.google.com/protocol-buffers/docs/proto3#any

Our `derr` lib should start handling both HTTP and gRPC error types.
  * the `gRPC` error types can include (within the `Details`) everything necessary to return
    a meaningful response to HTTP clients.  The proto defs could live in `derr` too.
A good read too: https://cloud.google.com/apis/design/errors

On handling errors in GraphQL: https://blog.apollographql.com/full-stack-error-handling-with-graphql-apollo-5c12da407210



Question

- 1) can an upstream grpc connection return an EOF error? how to handle it
- 2) Is a proper end of a GRPC stream an EOI
