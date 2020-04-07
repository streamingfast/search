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
	liveApp "github.com/dfuse-io/search/app/live"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func init() {
	serveLiveCmd.Flags().String("grpc-listen-addr", ":9000", "Address to listen for incoming gRPC requests")
	serveLiveCmd.Flags().String("block-stream-addr", ":9001", "gRPC Address to reach a stream of blocks")
	serveLiveCmd.Flags().String("live-indexes-path", "/tmp/live/indexes", "Location for live indexes (ideally a ramdisk)")
	serveLiveCmd.Flags().Int("truncation-threshold", 1, "number of available dmesh peers that should serve irreversible blocks before we truncate them from this backend's memory")
	serveLiveCmd.Flags().Duration("realtime-tolerance", 15*time.Second, "longest delay to consider this service as real-time(ready) on initialization")
	serveLiveCmd.Flags().Duration("shutdown-delay", 1*time.Second, "On shutdown, time to wait before actually leaving, to try and drain connections")
	serveLiveCmd.Flags().String("blockmeta-addr", ":9001", "Blockmeta endpoint is queried for its headinfo service")
	serveLiveCmd.Flags().Uint64("start-block-drift-tolerance", 500, "allowed number of blocks between search archive and network head to get start block from the search archive")
}

func serveLiveRunE(cmd *cobra.Command, args []string) (err error) {
	setup()
	checkRLimit()

	protocol := mustGetProtocol()

	dmeshEtcdCient := mustSetupEtcdDmesh()
	defer dmeshEtcdCient.Close()

	app := liveApp.New(&liveApp.Config{
		Dmesh:                    dmeshEtcdCient,
		Protocol:                 protocol,
		ServiceVersion:           viper.GetString("global-mesh-service-version"),
		TierLevel:                viper.GetUint32("global-mesh-search-tier-level"),
		GRPCListenAddr:           viper.GetString("serve-live-cmd-grpc-listen-addr"),
		PublishDuration:          viper.GetDuration("global-mesh-publish-polling-duration"),
		BlocksStoreURL:           viper.GetString("global-blocks-store"),
		BlockstreamAddr:          viper.GetString("serve-live-cmd-block-stream-addr"),
		HeadDelayTolerance:       viper.GetUint64("global-head-delay-tolerance"),
		DfuseHooksActionName:     viper.GetString("global-dfuse-hooks-action-name"),
		IndexingRestrictionsJSON: viper.GetString("global-indexing-restrictions-json"),
		LiveIndexesPath:          viper.GetString("serve-live-cmd-live-indexes-path"),
		TruncationThreshold:      viper.GetInt("serve-live-cmd-truncation-threshold"),
		RealtimeTolerance:        viper.GetDuration("serve-live-cmd-realtime-tolerance"),
		BlockmetaAddr:            viper.GetString("serve-live-cmd-blockmeta-addr"),
		StartBlockDriftTolerance: viper.GetUint64("serve-live-cmd-start-block-drift-tolerance"),
		ShutdownDelay:            viper.GetDuration("serve-live-cmd-shutdown-delay"),
	})

	zlog.Info("running search live application")
	derr.Check("error starting search live", app.Run())

	select {
	case <-app.Terminated():
		if err = app.Err(); err != nil {
			zlog.Error("search live shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		// TODO: should this propagate the signal errr
		err = fmt.Errorf("received signal %s", sig)
		app.Shutdown(err)
	}

	return err
}
