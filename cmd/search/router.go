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
	routerApp "github.com/dfuse-io/search/app/router"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func init() {
	routerCmd.Flags().String("listen-addr", ":9000", "Address to listen for incoming gRPC requests")
	routerCmd.Flags().String("blockmeta-addr", ":9001", "Blockmeta endpoint is queried to validate cursors that are passed LIB and forked out")
	routerCmd.Flags().Bool("enable-retry", false, "Enables the router's attempt to retry a backend search if there is an error. This could have adverse consequences when search through the live")
}

func routerRunE(cmd *cobra.Command, args []string) (err error) {
	setup()

	protocol := mustGetProtocol()

	dmeshEtcdCient := mustSetupEtcdDmesh()
	defer dmeshEtcdCient.Close()

	app := routerApp.New(&routerApp.Config{
		Dmesh:              dmeshEtcdCient,
		Protocol:           protocol,
		BlockmetaAddr:      viper.GetString("router-cmd-blockmeta-addr"),
		GRPCListenAddr:     viper.GetString("router-cmd-grpc-listen-addr"),
		HeadDelayTolerance: viper.GetUint64("global-head-delay-tolerance"),
		LibDelayTolerance:  viper.GetUint64("global-lib-delay-tolerance"),
		EnableRetry:        viper.GetBool("router-cmd-enable-retry"),
	})

	zlog.Info("running search archive application")
	derr.Check("error starting router", app.Run())

	select {
	case <-app.Terminated():
		if err = app.Err(); err != nil {
			zlog.Error("router shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		err = fmt.Errorf("received signal %s", sig)
		app.Shutdown(err)
	}

	return err
}
