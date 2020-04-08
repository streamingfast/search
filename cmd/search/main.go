// Copyright © 2018 EOS Canada <info@eoscanada.com>

package main

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/abourget/viperbind"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/logging"
	"github.com/dfuse-io/search"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
)

var zlog *zap.Logger

var rootCmd = &cobra.Command{Use: `analyze`, Short: "Print stats about a single index", RunE: analyzeRunE, Args: cobra.ExactArgs(1)}

func init() {
	rootCmd.PersistentFlags().IntP("shard-size", "s", 0, "Shard size to check integrity for")
	logging.Register("github.com/dfuse-io/search/cmd/search", &zlog)
	logging.Set(logging.MustCreateLoggerWithServiceName("search"))
}

func main() {
	runtime.SetMutexProfileFraction(10)
	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "SEARCH")
	})
	derr.Check("running analyze", rootCmd.Execute())
}

func analyzeRunE(cmd *cobra.Command, args []string) (err error) {
	shardSize := viper.GetInt("global-shard-size")

	if shardSize == 0 {
		return fmt.Errorf("specify --shard-size or -s")
	}

	errs := search.CheckIndexIntegrity(args[0], uint64(shardSize))
	if errs != nil {
		for _, err := range errs.(search.MultiError) {
			fmt.Println(err)
		}
		os.Exit(1)
	}

	return nil
}
