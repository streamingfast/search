module github.com/streamingfast/search

require (
	contrib.go.opencensus.io/exporter/stackdriver v0.13.8
	github.com/RoaringBitmap/roaring v0.4.23
	github.com/abourget/llerrgroup v0.2.0
	github.com/abourget/viperbind v0.1.0
	github.com/alecthomas/participle v0.7.1
	github.com/blevesearch/bleve v1.0.14
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/gorilla/mux v1.7.3
	github.com/magiconair/properties v1.8.1
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v0.0.7
	github.com/spf13/viper v1.6.2
	github.com/streamingfast/bstream v0.0.2-0.20210811181043-4c1920a7e3e3
	github.com/streamingfast/derr v0.0.0-20210811180100-9138d738bcec
	github.com/streamingfast/dgrpc v0.0.0-20210811180351-8646818518b2
	github.com/streamingfast/dmesh v0.0.0-20210811181323-5a37ad73216b
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20210811180812-4db13e99cc22
	github.com/streamingfast/logging v0.0.0-20210811175431-f3b44b61606a
	github.com/streamingfast/pbgo v0.0.6-0.20210811160400-7c146c2db8cc
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/goleveldb v1.0.1-0.20190923125748-758128399b1d // indirect
	go.opencensus.io v0.23.0
	go.uber.org/atomic v1.7.0
	go.uber.org/automaxprocs v1.3.0
	go.uber.org/zap v1.17.0
	google.golang.org/appengine v1.6.7
	google.golang.org/grpc v1.39.1
)

go 1.13
