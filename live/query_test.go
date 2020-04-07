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
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dfuse-io/bstream"
	_ "github.com/dfuse-io/bstream/codecs/deos"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	"github.com/dfuse-io/search"
	"github.com/eoscanada/eos-go"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
)

func setupIndexedBlocksSource(t *testing.T, handler bstream.Handler) (src *bstream.TestSource, closeFuncs []func()) {
	t.Helper()
	tmpDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	p := search.NewPreIndexer(search.NewEOSBlockMapper("dfuseiohooks:event", nil), tmpDir)
	preprocessor := bstream.PreprocessFunc(func(blk *bstream.Block) (interface{}, error) {
		obj, err := p.Preprocess(blk)
		if err != nil {
			return nil, err
		}
		closeFuncs = append(closeFuncs, func() {
			obj.(*search.SingleIndex).Delete()
		})
		return obj, nil
	})

	closeFuncs = append(closeFuncs, func() {
		os.RemoveAll(tmpDir)
	})

	src = bstream.NewTestSource(bstream.NewPreprocessor(preprocessor, handler))
	return

}

func getFakeHeadBlock() *bstream.Block {
	blk, err := ToBStreamBlock(newBlock("000000ffa", "000000fea", "abcdef123", "transfer"))
	if err != nil {
		panic(err)
	}

	return blk
}

func newBlock(id, previous, trxID string, account string) *pbdeos.Block {

	return &pbdeos.Block{
		Id:     id,
		Number: eos.BlockNum(id),
		Header: &pbdeos.BlockHeader{
			Previous:  previous,
			Timestamp: &timestamp.Timestamp{Nanos: 0, Seconds: 0},
		},
		TransactionTraces: []*pbdeos.TransactionTrace{
			{
				Id: trxID,
				Receipt: &pbdeos.TransactionReceiptHeader{
					Status: pbdeos.TransactionStatus_TRANSACTIONSTATUS_EXECUTED,
				},
				ActionTraces: []*pbdeos.ActionTrace{
					{
						Receipt: &pbdeos.ActionReceipt{
							Receiver: "receiver.1",
						},
						Action: &pbdeos.Action{
							Account: account,
							Name:    "transfer",
						},
					},
				},
			},
		},
	}
}

func bRef(id string) bstream.BlockRefFromID {
	return bstream.BlockRefFromID(id)
}

func trxID(num int) string {
	out := fmt.Sprintf("%d", num)
	for {
		out = fmt.Sprintf("%s.%d", out, num)
		if len(out) >= 32 {
			return out[:32]
		}
	}
}
func cursorFor(trxID string, blockID string) string {
	return fmt.Sprintf("1:%d:%s:%s", eos.BlockNum(blockID), blockID, trxID[:12])
}

func ToBStreamBlock(block *pbdeos.Block) (*bstream.Block, error) {
	time, _ := ptypes.Timestamp(block.Header.Timestamp)
	payload, err := proto.Marshal(block)
	if err != nil {
		return nil, err
	}
	return &bstream.Block{
		Id:             block.Id,
		Number:         uint64(block.Number),
		PreviousId:     block.PreviousID(),
		Timestamp:      time,
		LibNum:         block.LIBNum(),
		PayloadKind:    pbbstream.Protocol_EOS,
		PayloadVersion: 1,
		PayloadBuffer:  payload,
	}, nil
}
