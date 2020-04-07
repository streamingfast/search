// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"time"

	"github.com/dfuse-io/derr"
	archiveApp "github.com/dfuse-io/search/app/archive"
	roarcache "github.com/dfuse-io/search/archive/roarcache"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func init() {
	serveArchiveCmd.Flags().String("grpc-listen-addr", ":9000", "Address to listen for incoming gRPC requests")
	serveArchiveCmd.Flags().String("http-listen-addr", ":8080", "Address to listen for incoming http requests")
	serveArchiveCmd.Flags().String("memcache-addr", "localhost:11211", "Empty results cache's memcache server address")
	serveArchiveCmd.Flags().Bool("index-polling", false, "Populate local indexes using indexes store polling.")
	serveArchiveCmd.Flags().Bool("sync-from-storage", false, "Download missing indexes from --indexes-store before starting")
	serveArchiveCmd.Flags().Bool("enable-empty-results-cache", false, "Enable roaring-bitmap-based empty results caching")
	serveArchiveCmd.Flags().Duration("shutdown-delay", 1*time.Second, "On shutdown, time to wait before actually leaving, to try and drain connections")
	serveArchiveCmd.Flags().Int("sync-max-indexes", 20, "Maximum number of indexes to sync. On production, use a very large number.")
	serveArchiveCmd.Flags().Int("start-block", 0, "Start at given block num, the initial sync and polling")
	serveArchiveCmd.Flags().Uint("stop-block", 0, "Stop before given block num, the initial sync and polling")
	serveArchiveCmd.Flags().Int("indices-dl-threads", 4, "Number of indices files to download from the GS input store and decompress in parallel. In prod, use large value like 20.")
	serveArchiveCmd.Flags().Int("max-query-threads", 12, "Number of end-user query parallel threads to query 5K-blocks indexes")
	serveArchiveCmd.Flags().String("warmup-filepath", "", "Optional filename containing queries to warm-up the search")
	serveArchiveCmd.Flags().StringSlice("read-only-paths", []string{}, "Paths to shared indexes pools (for read-only indexes only)")
	serveArchiveCmd.Flags().Bool("enable-moving-tail", false, "Enable moving tail, requires a relative --start-block (negative number)")

}

func serveArchiveRunE(cmd *cobra.Command, args []string) (err error) {
	setup()
	checkRLimit()

	protocol := mustGetProtocol()

	dmeshEtcdCient := mustSetupEtcdDmesh()
	defer dmeshEtcdCient.Close()

	shardSize := viper.GetUint64("global-shard-size")
	memcacheAddr := viper.GetString("serve-archive-cmd-memcache-addr")
	enableEmptyResultsCache := viper.GetBool("serve-archive-cmd-enable-empty-results-cache")
	var cache roarcache.Cache
	if enableEmptyResultsCache {
		zlog.Info("setting up roar cache")
		cache = roarcache.NewMemcache(memcacheAddr, 30*24*time.Hour, shardSize)
	}

	app := archiveApp.New(&archiveApp.Config{
		Dmesh:            dmeshEtcdCient,
		Cache:            cache,
		ServiceVersion:   viper.GetString("global-mesh-service-version"),
		GRPCListenAddr:   viper.GetString("serve-archive-cmd-grpc-listen-addr"),
		HTTPListenAddr:   viper.GetString("serve-archive-cmd-http-listen-addr"),
		TierLevel:        viper.GetUint32("global-mesh-search-tier-level"),
		PublishDuration:  viper.GetDuration("global-mesh-publish-polling-duration"),
		ShutdownDelay:    viper.GetDuration("serve-archive-cmd-shutdown-delay"),
		EnableMovingTail: viper.GetBool("serve-archive-cmd-enable-moving-tail"),
		IndexesStoreURL:  viper.GetString("global-indexes-store"),
		IndexesPath:      viper.GetString("global-writable-path"),
		ShardSize:        shardSize,
		StartBlock:       viper.GetInt64("serve-archive-cmd-start-block"),
		StopBlock:        viper.GetUint64("serve-archive-cmd-stop-block"),
		SyncFromStore:    viper.GetBool("serve-archive-cmd-sync-from-storage"),
		SyncMaxIndexes:   viper.GetInt("serve-archive-cmd-sync-max-indexes"),
		IndicesDLThreads: viper.GetInt("serve-archive-cmd-indices-dl-threads"),
		NumQueryThreads:  viper.GetInt("serve-archive-cmd-max-query-threads"),
		IndexPolling:     viper.GetBool("serve-archive-cmd-index-polling"),
		Protocol:         protocol,
		WarmupFilepath:   viper.GetString("serve-archive-cmd-warmup-filepath"),
	})

	zlog.Info("running search archive application")
	derr.Check("error starting search archive", app.Run())

	select {
	case <-app.Terminated():
		if err = app.Err(); err != nil {
			zlog.Error("search archive shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		err = fmt.Errorf("received signal %s", sig)
		app.Shutdown(err)
	}

	return err
}
