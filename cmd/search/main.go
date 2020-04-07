// Copyright © 2018 EOS Canada <info@eoscanada.com>

package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/abourget/viperbind"
	"github.com/dfuse-io/derr"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{Use: "search", Short: "Operate the dfuse Search Engine"}
var serveArchiveCmd = &cobra.Command{Use: "serve-archive", Short: "Run the archive backend, index in real-time and serve queries", RunE: serveArchiveRunE}
var serveLiveCmd = &cobra.Command{Use: "serve-live", Short: "Run the live backend and serves it", RunE: serveLiveRunE}
var serveForkResolverCmd = &cobra.Command{Use: "serve-fork-resolver", Short: "Run the fork-resolver backend and serves it", RunE: serveForkResolverRunE}
var routerCmd = &cobra.Command{Use: "router", Short: "Run the live-router server", RunE: routerRunE}
var indexerCmd = &cobra.Command{Use: "indexer", Short: "Process block indexes", RunE: indexerRunE}
var analyzeCmd = &cobra.Command{Use: `analyze`, Short: "Print stats about a single index", RunE: analyzeRunE, Args: cobra.ExactArgs(1)}

func main() {
	runtime.SetMutexProfileFraction(10)

	// StackDriver Profiler
	//err := profiler.Start(profiler.Config{
	//	Service:              "search",
	//	ServiceVersion:       "0.1.0",
	//	DebugLogging:         false,
	//	MutexProfiling:       false,
	//	AllocForceGC:         false,
	//	NoAllocProfiling:     true,
	//	NoHeapProfiling:      true,
	//	NoGoroutineProfiling: true,
	//})
	//derr.ErrorCheck("Cannot start StackDriver Profiler", err)

	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "SEARCH")
		startupDelay := viper.GetDuration("global-startup-delay")
		if startupDelay != 0 {
			zlog.Info("waiting for startup delay before starting actual command", zap.Duration("delay", startupDelay))
			time.Sleep(startupDelay)
		}

	})

	rootCmd.PersistentFlags().String("writable-path", "/tmp/bleve-indexes", "Writable base path for storing index files")
	rootCmd.PersistentFlags().Uint("shard-size", 5000, "Number of blocks to store in a given Bleve index")
	rootCmd.PersistentFlags().Duration("shutdown-drain-delay", 30*time.Second, "Delay between signal to shutdown and actual shutdown (while healthz returns NOT_SERVING)")
	rootCmd.PersistentFlags().String("indexes-store", "gs://example/indexes", "GS path to read or write index shards")
	rootCmd.PersistentFlags().String("blocks-store", "gs://example/blocks", "GS path to read blocks archives")
	rootCmd.PersistentFlags().String("dfuse-hooks-action-name", "dfuseiohooks:event", "The dfuse Hooks event action name to intercept")
	rootCmd.PersistentFlags().String("indexing-restrictions-json", "", "json-formatted array of items to skip from indexing")
	rootCmd.PersistentFlags().String("protocol", "", "Protocol in string, must fit with bstream.ProtocolRegistry")
	rootCmd.PersistentFlags().Uint64("head-delay-tolerance", 3, "Number of blocks above a backend's head we allow a request query to be served (Live & Router)")
	rootCmd.PersistentFlags().Uint64("lib-delay-tolerance", 12, "Number of blocks above a backend's lib we allow a request query to be served (Live & Router)")
	rootCmd.PersistentFlags().Duration("startup-delay", 0, "An artifical delay to wait for before starting the actual command, can be used to perform some maintenance work on the pod")

	// mesh parameters
	rootCmd.PersistentFlags().String("mesh-store-addr", ":2379", "address of the backing etcd cluster for mesh service discovery")
	rootCmd.PersistentFlags().String("mesh-namespace", "dev-dev", "dmesh namespace where services reside (eos-mainnet)")
	rootCmd.PersistentFlags().String("mesh-service-version", "v1", "dmesh service version (v1)")
	rootCmd.PersistentFlags().Uint32("mesh-search-tier-level", 10, "level of the search tier")

	rootCmd.AddCommand(serveArchiveCmd)
	rootCmd.AddCommand(indexerCmd)
	rootCmd.AddCommand(routerCmd)
	rootCmd.AddCommand(serveLiveCmd)
	rootCmd.AddCommand(serveForkResolverCmd)
	rootCmd.AddCommand(analyzeCmd)

	derr.Check("running search", rootCmd.Execute())
}

func mustSetupEtcdDmesh() *dmeshClient.Etcd {
	storeAddr := viper.GetString("global-mesh-store-addr")
	namespace := viper.GetString("global-mesh-namespace")
	serviceVersion := viper.GetString("global-mesh-service-version")

	zlog.Info("setting up dmesh",
		zap.String("mesh_store_addr", storeAddr),
		zap.String("mesh_namespace", namespace),
		zap.String("mesh_service_version", serviceVersion))

	client, err := dmeshClient.NewEtcdStore(storeAddr)
	derr.Check("unable to setup dmesh store (etcd)", err)
	mesh := dmeshClient.NewEtcd(context.Background(), namespace, client, []string{
		"/" + serviceVersion + "/search",
	})

	err = mesh.Start()
	derr.Check("unable to start mesh", err)
	zlog.Info("mesh started")

	return mesh
}

func mustGetProtocol() pbbstream.Protocol {
	protocolString := viper.GetString("global-protocol")
	if protocolString == "" {
		fmt.Println("You must pass the 'protocol' flag, none received")
		os.Exit(1)
	}

	protocol := pbbstream.Protocol(pbbstream.Protocol_value[protocolString])
	if protocol == pbbstream.Protocol_UNKNOWN {
		derr.Check("invalid protocol", fmt.Errorf("protocol value: %q", protocolString))
	}

	// TODO: check that we did _underscore-import_ the right protocol
	// for what is selected.

	return protocol
}

func checkRLimit() {
	if os.Getenv("SEARCH_SKIP_RLIMIT") == "true" {
		return
	}

	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	derr.Check("failed to check rlimits", err)

	if rLimit.Cur < 100000 {
		derr.Check("run `ulimit -n 1000000` before launching search service", fmt.Errorf("max open files too low: %d", rLimit.Cur))
	}
}
