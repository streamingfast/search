// Copyright © 2018 EOS Canada <info@eoscanada.com>

package main

import (
	"fmt"
	_ "net/http/pprof"
	"regexp"
	"runtime"
	"strconv"

	"github.com/abourget/viperbind"
	"github.com/blevesearch/bleve/index/scorch"
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
	logging.Register("github.com/dfuse-io/search/cmd/search", &zlog)
	logging.Set(logging.MustCreateLoggerWithServiceName("search"))
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

	if shardSize == 0 {
		return fmt.Errorf("specify --shard-size or -s")
	}

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
	)

	idx, err := scorch.NewScorch("data", map[string]interface{}{
		"read_only": true,
		"path":      bleeveIndexPath,
	}, nil)
	if err != nil {
		return fmt.Errorf("unable to create scorch index: %w", err)
	}
	err = idx.Open()
	if err != nil {
		return fmt.Errorf("unable to open scorch index: %s", err)
	}

	shard := &search.ShardIndex{
		StartBlock: bleeveIndexBaseBlock,
		EndBlock:   bleeveIndexBaseBlock + uint64(shardSize) - 1,
		Index:      idx,
	}

	start, end, err := shard.GetBoundaryBlocks(idx)
	if err != nil {
		return fmt.Errorf("unable get boundaries: %s", err)
	}

	zlog.Info("index boundary information",
		zap.Time("start_block_time", start.Time),
		zap.String("start_block_id", start.ID),
		zap.Uint64("start_block_num", start.Num),
		zap.Time("end_block_time", end.Time),
		zap.String("end_block_id", end.ID),
		zap.Uint64("end_block_num", end.Num),
	)

	errs := search.CheckIndexIntegrity(bleeveIndexPath, uint64(shardSize))
	if errs != nil {
		for _, err := range errs.(search.MultiError) {
			zlog.Info("integrity checked failed", zap.Error(err))
		}
	}

	return nil
}
