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

	"github.com/dfuse-io/derr"
	forkresolverApp "github.com/dfuse-io/search/app/forkresolver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func init() {
	serveForkResolverCmd.Flags().String("grpc-listen-addr", ":9000", "Address to listen for incoming gRPC requests")
	serveForkResolverCmd.Flags().String("indices-path", "/tmp/forkresolver/indices", "Location for inflight indices")
}

func serveForkResolverRunE(cmd *cobra.Command, args []string) (err error) {
	setup()

	protocol := mustGetProtocol()

	dmeshEtcdCient := mustSetupEtcdDmesh()
	defer dmeshEtcdCient.Close()

	app := forkresolverApp.New(&forkresolverApp.Config{
		Dmesh:                    dmeshEtcdCient,
		Protocol:                 protocol,
		ServiceVersion:           viper.GetString("global-mesh-service-version"),
		GRPCListenAddr:           viper.GetString("serve-fork-resolver-cmd-grpc-listen-addr"),
		PublishDuration:          viper.GetDuration("global-mesh-publish-polling-duration"),
		IndicesPath:              viper.GetString("serve-fork-resolver-cmd-indices-path"),
		BlocksStoreURL:           viper.GetString("global-blocks-store"),
		DfuseHooksActionName:     viper.GetString("global-dfuse-hooks-action-name"),
		IndexingRestrictionsJSON: viper.GetString("global-indexing-restrictions-json"),
	})

	zlog.Info("running search forkresolver application")
	derr.Check("error starting search forkresolver", app.Run())

	select {
	case <-app.Terminated():
		if err = app.Err(); err != nil {
			zlog.Error("search forkresolver shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		err = fmt.Errorf("received signal %s", sig)
		app.Shutdown(err)
	}

	return err
}
