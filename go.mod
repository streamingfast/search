module github.com/dfuse-io/search

require (
	contrib.go.opencensus.io/exporter/stackdriver v0.12.6
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/abourget/llerrgroup v0.2.0
	github.com/abourget/viperbind v0.1.0
	github.com/alecthomas/participle v0.2.0
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/blevesearch/bleve v0.8.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/dfuse-io/bstream v0.0.0-20200414225043-302ba2b0a512
	github.com/dfuse-io/derr v0.0.0-20200406214256-c690655246a1
	github.com/dfuse-io/dfuse-eosio v0.0.0-20200414182521-be119d403f4c
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmesh v0.0.0-20200407045015-ea4e41ecdb6c
	github.com/dfuse-io/dmetrics v0.0.0-20200406214800-499fc7b320ab
	github.com/dfuse-io/dstore v0.0.0-20200407173215-10b5ced43022
	github.com/dfuse-io/logging v0.0.0-20200407175011-14021b7a79af
	github.com/dfuse-io/pbgo v0.0.6-0.20200407175820-b82ffcb63bf6
	github.com/dfuse-io/shutter v1.4.1-0.20200407040739-f908f9ab727f
	github.com/eoscanada/eos-go v0.9.1-0.20200401171810-21f9a1430901
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/gorilla/mux v1.7.3
	github.com/magiconair/properties v1.8.1
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v0.0.7
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.3
	go.uber.org/atomic v1.6.0
	go.uber.org/automaxprocs v1.3.0
	go.uber.org/zap v1.14.0
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413
	google.golang.org/appengine v1.6.5
	google.golang.org/grpc v1.26.0
)

replace github.com/blevesearch/bleve => github.com/fproulx-eoscanada/bleve v0.0.0-20190823192325-db63d5f16d8b

go 1.13
