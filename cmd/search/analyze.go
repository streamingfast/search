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
	"os"

	"github.com/dfuse-io/search"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	analyzeCmd.Flags().IntP("shard-size", "s", 0, "Shard size to check integrity for")
}

func analyzeRunE(cmd *cobra.Command, args []string) (err error) {
	shardSize := viper.GetInt("analyze-cmd-shard-size")

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
