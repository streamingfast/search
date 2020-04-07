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

package live

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/dfuse-io/bstream/forkable"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_forwardProcessBlock(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	preIndexer := search.NewPreIndexer(search.NewEOSBlockMapper("dfuseiohooks:event", nil), tmpDir)

	cases := []struct {
		name                  string
		block                 *pbdeos.Block
		expectedMatchCount    int
		expectedLastBlockRead uint64
		cancelContext         bool
		expectedError         string
	}{
		{
			name:                  "sunny path",
			block:                 newBlock("00000006a", "00000005a", trxID(2), "eosio.token"),
			expectedLastBlockRead: uint64(6),
			expectedMatchCount:    1,
		},
		{
			name:               "canceled context",
			block:              newBlock("00000006a", "00000005a", trxID(2), "eosio.token"),
			cancelContext:      true,
			expectedMatchCount: 0,
			expectedError:      "rpc error: code = Canceled desc = context canceled",
		},
		{
			name:               "block to young context",
			block:              newBlock("00000009a", "00000001a", trxID(2), "eosio.token"),
			expectedMatchCount: 0,
			expectedError:      "end of block range",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			block, err := ToBStreamBlock(c.block)
			require.NoError(t, err)
			preprocessObj, err := preIndexer.Preprocess(block)

			fObj := &forkable.ForkableObject{
				Obj: preprocessObj.(*search.SingleIndex),
			}

			bleveQuery, err := search.NewParsedQuery(pbbstream.Protocol_EOS, "account:eosio.token")
			matchCollector := search.MatchCollectorByType[pbbstream.Protocol_EOS]
			if matchCollector == nil {
				panic(fmt.Errorf("no match collector for protocol %s, should not happen, you should define a collector", pbbstream.Protocol_EOS))
			}

			incomingMatches := make(chan *pb.SearchMatch)

			q := liveQuery{
				bleveQuery: bleveQuery,
				request: &pb.BackendRequest{
					LowBlockNum:  0,
					HighBlockNum: uint64(8),
				},
			}

			matchesReceived := make(chan bool)
			var matches []*pb.SearchMatch
			if c.expectedMatchCount > 0 {
				go func() {
					select {
					case m := <-incomingMatches:
						matches = append(matches, m)
						if len(matches) == c.expectedMatchCount {
							close(matchesReceived)
						}
					case <-time.After(100 * time.Millisecond):
						close(matchesReceived)
					}
				}()
			} else {
				close(matchesReceived)
			}

			ctx := context.Background()
			if c.cancelContext {
				canceledContext, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceledContext
			}
			q.ctx = ctx
			q.matchCollector = matchCollector
			q.incomingMatches = incomingMatches
			err = q.forwardProcessBlock(block, fObj)
			if c.expectedError != "" {
				require.Error(t, err)
				require.Equal(t, c.expectedError, err.Error())
				return
			}

			require.NoError(t, err)
			<-matchesReceived
			assert.Equal(t, c.expectedLastBlockRead, q.lastBlockRead)
			assert.Len(t, matches, c.expectedMatchCount)
		})
	}

}

func Test_processMatches(t *testing.T) {
	cases := []struct {
		name               string
		block              *pbdeos.Block
		liveQuery          *liveQuery
		matches            []search.SearchMatch
		expectedMatchCount int
	}{
		{
			name:               "With Match no marker",
			liveQuery:          &liveQuery{},
			block:              newBlock("00000006a", "00000005a", trxID(2), "eosio.token"),
			expectedMatchCount: 1,
			matches: []search.SearchMatch{
				&search.EOSSearchMatch{},
			},
		},
		{
			name: "With Match and marker",
			liveQuery: &liveQuery{
				liveMarkerReached:          true,
				liveMarkerLastSentBlockNum: 1,
				request: &pb.BackendRequest{
					LiveMarkerInterval: 2,
				},
			},
			matches: []search.SearchMatch{
				&search.EOSSearchMatch{},
			},
			block:              newBlock("00000006a", "00000005a", trxID(2), "eosio.token"),
			expectedMatchCount: 2,
		},
		{
			name: "No match and marker",
			liveQuery: &liveQuery{
				liveMarkerReached:          true,
				liveMarkerLastSentBlockNum: 1,
				request: &pb.BackendRequest{
					LiveMarkerInterval: 2,
				},
			},
			block:              newBlock("00000006a", "00000005a", trxID(2), "eosio.token"),
			expectedMatchCount: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			block, err := ToBStreamBlock(c.block)
			require.NoError(t, err)

			ctx := context.Background()

			c.liveQuery.ctx = ctx

			incomingMatches := make(chan *pb.SearchMatch)
			doneReceiving := make(chan bool)

			var matchesReceived []*pb.SearchMatch
			if c.expectedMatchCount > 0 {
				go func() {
					for {
						select {
						case m := <-incomingMatches:
							matchesReceived = append(matchesReceived, m)
							if len(matchesReceived) == c.expectedMatchCount {
								close(doneReceiving)
							}
						}
					}
				}()
			} else {
				close(doneReceiving)
			}

			c.liveQuery.incomingMatches = incomingMatches

			err = c.liveQuery.processMatches(c.matches, block, uint64(5), forkable.StepNew)
			require.NoError(t, err)
			<-doneReceiving

			assert.Len(t, matchesReceived, c.expectedMatchCount)

		})
	}

}
