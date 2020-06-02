module github.com/dfuse-io/search

require (
	contrib.go.opencensus.io/exporter/stackdriver v0.12.6
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/abourget/llerrgroup v0.2.0
	github.com/abourget/viperbind v0.1.0
	github.com/alecthomas/participle v0.2.0
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/blevesearch/bleve v1.0.9
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cznic/b v0.0.0-20181122101859-a26611c4d92d // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537 // indirect
	github.com/dfuse-io/bstream v0.0.0-20200522154238-b71cafb7ad1a
	github.com/dfuse-io/derr v0.0.0-20200406214256-c690655246a1
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmesh v0.0.0-20200427143025-f55305fa4b95
	github.com/dfuse-io/dmetrics v0.0.0-20200406214800-499fc7b320ab
	github.com/dfuse-io/dstore v0.1.0
	github.com/dfuse-io/logging v0.0.0-20200407175011-14021b7a79af
	github.com/dfuse-io/pbgo v0.0.6-0.20200602201455-99986ef5a09d
	github.com/dfuse-io/shutter v1.4.1-0.20200407040739-f908f9ab727f
	github.com/facebookgo/ensure v0.0.0-20200202191622-63f1cf65ac4c // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4 // indirect
	github.com/golang/protobuf v1.3.5 // indirect
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/gorilla/mux v1.7.3
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/magiconair/properties v1.8.1
	github.com/pkg/errors v0.9.1
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/sergi/go-diff v1.0.0 // indirect
	github.com/spf13/cobra v0.0.7
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.4.0
	github.com/syndtr/goleveldb v1.0.1-0.20190923125748-758128399b1d // indirect
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c // indirect
	go.opencensus.io v0.22.3
	go.uber.org/atomic v1.6.0
	go.uber.org/automaxprocs v1.3.0
	go.uber.org/zap v1.14.0
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413
	google.golang.org/appengine v1.6.5
	google.golang.org/grpc v1.26.0
)

go 1.13
