## Development environment

`search` uses Go 1.13's `modules`. Init your `git` with:

    git config --global url.ssh://git@github.com.insteadof https://github.com

and store the `search` repository OUTSIDE of your GOPATH. (otherwise
you'll need to fiddle with `GO111MODULE=on` but that might conflict
your other repos)

## Development Setup

First open a port forward to devproxy:

    kubectl -n eth-mainnet port-forward deploy/devproxy 9001

Secondly, open a port forward to dmesh:

    kubectl -n dmesh port-forward svc/etcd-client 2379

### Frozen Archive Backend  from 0 to 10k
```shell script
go build -o sqe ./cmd/search && \
./sqe serve-archive \
    --listen-addr=:9000 \
    --protocol=EOS \
    --blockmeta-addr=:9001 \
    --block-stream-addr=:9001 \
    --blocks-store=gs://dfuseio-global-blocks-us/eos-dev1/v3 \
    --indexes-store=gs://dfuseio-global-indices-us/eos-dev1/v2-0 \
    --dfuse-hooks-action-name=dfuseiohooks:event \
    --dl-threads=2 \
    --max-query-threads=12 \
    --realtime-tolerance=10s \
    --shard-size=5000 \
    --sync-from-storage \
    --sync-start-block=0 \
    --sync-max-indexes=2 \
    --index-pipeline=false
```

### Live Archive Backend

```shell script
go build -o sqe ./cmd/search && \
./sqe serve-live \
  --listen-addr=:9000 \
  --block-stream-addr=:9001 \
  --blocks-store=gs://dfuseio-global-blocks-us/eos-dev1/v3 \
  --protocol=EOS \
  --truncation-threshold=2
```

### Router

```shell script
go build -o sqe ./cmd/search && \
./sqe router \
  --listen-addr=:9000 \
  --protocol=EOS
```

echo '{
    "query": "action:onblock",
    "lowBlockNum":  44810200,
    "highBlockNum": 44810250,
    "lowBlockUnbounded": false,
    "highBlockUnbounded": false,
    "descending": false,
    "withReversible": true
}' |  grpcurl -plaintext -d @ localhost:9000 dfuse.search.v1.Router/StreamMatches


### Sample Router Query

```shell script
echo '{
    "query": "action:onblock",
    "lowBlockNum":  44850200,
    "highBlockNum": 44850250,
    "lowBlockUnbounded": false,
    "highBlockUnbounded": false,
    "descending": false,
    "withReversible": true
}' |  grpcurl -plaintext -d @ localhost:9000 dfuse.search.v1.Router/StreamMatches
```

```shell script
echo '{
    "query": "action:onblock",
    "lowBlockNum":  84850200,
    "highBlockNum": 84850250,
    "lowBlockUnbounded": false,
    "highBlockUnbounded": false,
    "descending": false,
    "withReversible": true
}' |  grpcurl -plaintext -d @ localhost:9000 dfuse.search.v1.Router/StreamMatches
```

```shell script
echo '{
    "query": "action:onblock",
    "lowBlockNum":  86999950,
    "highBlockNum": 87000050,
    "lowBlockUnbounded": false,
    "highBlockUnbounded": false,
    "descending": false,
    "withReversible": true
}' |  grpcurl -plaintext -d @ localhost:9000 dfuse.search.v1.Router/StreamMatches
```

```shell script
echo '{
    "query": "receiver:newdexpublic action:traderecord",
    "lowBlockNum":  83206460,
    "highBlockNum": 83306470,
    "lowBlockUnbounded": false,
    "highBlockUnbounded": false,
    "descending": false,
    "withReversible": true
}' |  grpcurl -plaintext -d @ localhost:9000 dfuse.search.v1.Router/StreamMatches
```

```shell script
echo '{
    "query": "action:onblock",
    "lowBlockNum":  -1,
    "highBlockNum": -1,
    "lowBlockUnbounded": false,
    "highBlockUnbounded": true,
    "descending": false,
    "withReversible": true
}' |  grpcurl -plaintext -d @ localhost:9000 dfuse.search.v1.Router/StreamMatches
```
## Customer examples


REPLACE `eoscafeblock` for the user

curl "http://staging-mainnet.eos.dfuse.io/v0/search/transactions?q=action:claimrewards%20data.owner:eoscanadacom&limit=20&start_block=100&block_count=30000000&token=$DFUSE"

where `q` looks like:

`action:actionname account:accountname data.somekey:somevalue`

or

`(action:issue OR action:transfer) account:eosio`

or

`account:eosio.token receiver:eosio.token (data.from:eoscanadacom OR data.to:eoscanadacom)`

simulate the `history_api` semantics:

`(auth:ACCOUNT OR receiver:ACCOUNT)`
