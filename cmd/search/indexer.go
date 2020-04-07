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
	"time"

	"github.com/dfuse-io/derr"
	indexerApp "github.com/dfuse-io/search/app/indexer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func init() {
	indexerCmd.Flags().String("grpc-listen-addr", ":9000", "Address to listen for incoming gRPC requests")
	indexerCmd.Flags().String("http-listen-addr", ":8080", "Address to listen for incoming http requests")
	indexerCmd.Flags().Bool("enable-upload", false, "Upload merged indexes to the --indexes-store")
	indexerCmd.Flags().Bool("delete-after-upload", false, "Delete local indexes after uploading them")
	indexerCmd.Flags().String("block-stream-addr", ":9001", "gRPC URL to reach a stream of blocks")
	indexerCmd.Flags().String("blockmeta-addr", ":9001", "Blockmeta endpoint is queried to find the last irreversible block on the network being indexed")
	indexerCmd.Flags().Int("start-block", 0, "Start indexing from block num")
	indexerCmd.Flags().Uint("stop-block", 0, "Stop indexing at block num")
	indexerCmd.Flags().Bool("enable-batch-mode", false, "Enabled the indexer in batch mode with a start & stoip block")
	indexerCmd.Flags().Uint("num-blocks-before-start", 500, "Number of blocks to fetch before start block")
	indexerCmd.Flags().Bool("verbose", false, "Verbose logging")
	indexerCmd.Flags().Bool("enable-index-truncation", false, "Enable index truncation, requires a relative --start-block (negative number)")
}

func indexerRunE(cmd *cobra.Command, args []string) (err error) {
	setup()

	protocol := mustGetProtocol()

	app := indexerApp.New(&indexerApp.Config{
		GRPCListenAddr:                      viper.GetString("indexer-cmd-grpc-listen-addr"),
		HTTPListenAddr:                      viper.GetString("indexer-cmd-http-listen-addr"),
		IndexesStoreURL:                     viper.GetString("global-indexes-store"),
		BlocksStoreURL:                      viper.GetString("global-blocks-store"),
		Protocol:                            protocol,
		BlockstreamAddr:                     viper.GetString("indexer-cmd-block-stream-addr"),
		DfuseHooksActionName:                viper.GetString("global-dfuse-hooks-action-name"),
		IndexingRestrictionsJSON:            viper.GetString("global-indexing-restrictions-json"),
		WritablePath:                        viper.GetString("global-writable-path"),
		ShardSize:                           viper.GetUint64("global-shard-size"),
		StartBlock:                          viper.GetInt64("indexer-cmd-start-block"),
		StopBlock:                           viper.GetUint64("indexer-cmd-stop-block"),
		IsVerbose:                           viper.GetBool("indexer-cmd-verbose"),
		EnableBatchMode:                     viper.GetBool("indexer-cmd-enable-batch-mode"),
		BlockmetaAddr:                       viper.GetString("indexer-cmd-blockmeta-addr"),
		NumberOfBlocksToFetchBeforeStarting: viper.GetUint64("indexer-cmd-num-blocks-before-start"),
		EnableUpload:                        viper.GetBool("indexer-cmd-enable-upload"),
		DeleteAfterUpload:                   viper.GetBool("indexer-cmd-delete-after-upload"),
		EnableIndexTruncation:               viper.GetBool("indexer-cmd-enable-index-truncation"),
	})

	zlog.Info("running search indexer application")
	derr.Check("error starting search indexer", app.Run())

	select {
	case <-app.Terminated():
		if err = app.Err(); err != nil {
			zlog.Error("search indexer shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		app.Shutdown(nil)
	}

	zlog.Info("adding sleep time after indexer run")
	time.Sleep(30 * time.Second)
	return err
}
