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

package archive

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/search"
	"github.com/stretchr/testify/require"
)

func init() {
	search.GetMatchCollector = search.TestMatchCollector
}
func TestRunQueryMainnet60M(t *testing.T) {
	t.Skip("run fetch.sh to download the test index, and comment this line.")
	pool := &IndexPool{
		ShardSize:       5000,
		PerQueryThreads: 2,
	}
	pool.LowestServeableBlockNum = 60000000

	idx, err := pool.openReadOnly(60000000)
	require.NoError(t, err)

	pool.ReadPool = append(pool.ReadPool, idx)

	client, cleanup := TestNewClient(t, &ArchiveBackend{Pool: pool, MaxQueryThreads: 2})
	defer cleanup()

	queries, err := readLines("testdata/60M-mainnet-index/raw_queries_sort_uniq.txt")
	require.NoError(t, err)
	for idx, query := range queries {

		fmt.Println("Q:", query)

		t1 := time.Now()
		for linear := 0; linear < 20; linear++ {

			wg := sync.WaitGroup{}
			for i := 0; i < 1; i++ {
				wg.Add(1)

				i := i
				idx := idx

				go func() {
					t0 := time.Now()
					resp, err := client.StreamMatches(context.Background(), &pb.BackendRequest{
						Query:        query,
						LowBlockNum:  60000000,
						HighBlockNum: 60004999,
						//Limit:          100,
						Descending:     false,
						WithReversible: false,
					})
					require.NoError(t, err)

					var out []interface{}
					for {
						el, err := resp.Recv()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
						out = append(out, el)
					}
					fmt.Println("TIMING", idx, i, time.Since(t0))
					wg.Done()
				}()
			}
			wg.Wait()
		}
		fmt.Println("TOTAL", idx, time.Since(t1))
		fmt.Println("-------------")
	}

	require.NoError(t, pool.CloseIndexes())
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
