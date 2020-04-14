module github.com/dfuse-io/search

require (
	contrib.go.opencensus.io/exporter/stackdriver v0.12.6
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/abourget/llerrgroup v0.2.0
	github.com/abourget/viperbind v0.1.0
	github.com/alecthomas/participle v0.2.0
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/blevesearch/bleve v0.8.0
	github.com/blevesearch/blevex v0.0.0-20190916190636-152f0fe5c040 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cznic/b v0.0.0-20181122101859-a26611c4d92d // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537 // indirect
	github.com/dfuse-io/bstream v0.0.0-20200406220134-fe85c256872f
	github.com/dfuse-io/derr v0.0.0-20200406214256-c690655246a1
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmesh v0.0.0-20200407045015-ea4e41ecdb6c
	github.com/dfuse-io/dmetrics v0.0.0-20200406214800-499fc7b320ab
	github.com/dfuse-io/dstore v0.0.0-20200407173215-10b5ced43022
	github.com/dfuse-io/dtracing v0.0.0-20200406213603-4b0c0063b125
	github.com/dfuse-io/jsonpb v0.0.0-20200406211248-c5cf83f0e0c0
	github.com/dfuse-io/logging v0.0.0-20200406213449-45fc25dc6a8d
	github.com/dfuse-io/pbgo v0.0.6-0.20200325181437-64bdab32d1b7
	github.com/dfuse-io/shutter v1.4.1-0.20200319040708-c809eec458e6
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/eoscanada/eos-go v0.9.1-0.20200227221642-1b19518201a1
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/gorilla/mux v1.7.0
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/magiconair/properties v1.8.0
	github.com/pkg/errors v0.9.1
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.4.0
	github.com/stretchr/testify v1.4.0
	github.com/tecbot/gorocksdb v0.0.0-20190705090504-162552197222 // indirect
	go.opencensus.io v0.22.2
	go.uber.org/atomic v1.6.0
	go.uber.org/automaxprocs v1.3.0
	go.uber.org/zap v1.14.0
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/tools v0.0.0-20200331025713-a30bf2db82d4 // indirect
	google.golang.org/appengine v1.6.1
	google.golang.org/grpc v1.26.0
	honnef.co/go/tools v0.0.1-2020.1.3 // indirect
)

replace github.com/blevesearch/bleve => github.com/fproulx-eoscanada/bleve v0.0.0-20190823192325-db63d5f16d8b

go 1.13
