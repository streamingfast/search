// Copyright © 2018 EOS Canada <info@eoscanada.com>

package main

import (
	"fmt"
	_ "net/http/pprof"
	"regexp"
	"runtime"
	"strconv"

	"github.com/abourget/viperbind"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/logging"
	"github.com/dfuse-io/search"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"
)

var zlog *zap.Logger

var analyzeCmd = &cobra.Command{Use: "analyze", Short: "Print stats about a single index", RunE: analyzeRunE, Args: cobra.ExactArgs(1)}

func init() {
	analyzeCmd.PersistentFlags().IntP("shard-size", "s", 0, "Shard size to check integrity for")
	analyzeCmd.PersistentFlags().Int("protocol-first-block", 0, "Protocol's lowest block number")
	logging.Register("github.com/dfuse-io/search/cmd/analyze", &zlog)
	logging.Set(logging.MustCreateLoggerWithServiceName("search-analyze"))
}

func main() {
	runtime.SetMutexProfileFraction(10)
	cobra.OnInitialize(func() {
		viperbind.AutoBind(analyzeCmd, "SEARCH")
	})
	derr.Check("running analyze", analyzeCmd.Execute())
}

func analyzeRunE(cmd *cobra.Command, args []string) (err error) {
	shardSize := viper.GetInt("global-shard-size")
	protocolFirstBlock := viper.GetInt("global-protocol-first-block")

	if shardSize == 0 {
		return fmt.Errorf("specify --shard-size or -s")
	}

	bstream.GetProtocolFirstStreamableBlock = uint64(protocolFirstBlock)

	bleeveIndexPath := args[0]

	match := regexp.MustCompile(`(\d{10})\.bleve`).FindStringSubmatch(bleeveIndexPath)

	if match == nil {
		return fmt.Errorf("unable to retrieve base block from bleve filename")
	}

	bleeveIndexBaseBlock, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil {
		return err
	}
	zlog.Info("analyzing index",
		zap.String("index", bleeveIndexPath),
		zap.Uint64("base_block_num", bleeveIndexBaseBlock),
		zap.Int("shard_size", shardSize),
		zap.Uint64("protocol_first_block", bstream.GetProtocolFirstStreamableBlock),
	)
	metaInfo, errs := search.CheckIndexIntegrity(bleeveIndexPath, uint64(shardSize))
	if errs != nil {
		for _, err := range errs.(search.MultiError) {
			zlog.Warn("[ERROR] integrity checked failed", zap.Error(err))
		}
	}

	if metaInfo == nil {
		return nil
	}

	zlog.Info("index block information", zap.Uint64("lowest_block", metaInfo.LowestBlockNum), zap.Uint64("highest_block", metaInfo.HighestBlockNum))
	if metaInfo.StartBlock != nil {
		zlog.Info("index start boundary information", zap.Time("start_block_time", metaInfo.StartBlock.Time), zap.String("start_block_id", metaInfo.StartBlock.ID), zap.Uint64("start_block_num", metaInfo.StartBlock.Num))
	}
	if metaInfo.EndBlock != nil {
		zlog.Info("index end boundary information", zap.Time("end_block_time", metaInfo.EndBlock.Time), zap.String("end_block_id", metaInfo.EndBlock.ID), zap.Uint64("end_block_num", metaInfo.EndBlock.Num))
	}

	return nil
}
