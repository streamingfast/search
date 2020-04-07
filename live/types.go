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
	pb "github.com/dfuse-io/pbgo/dfuse/search/v1"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/search"
)

func liveSearchMatchToProto(blk *bstream.Block, irrBlockNum uint64, undo bool, match search.SearchMatch) (*pb.SearchMatch, error) {
	pbMatch := &pb.SearchMatch{
		TrxIdPrefix: match.TransactionIDPrefix(),
		Index:       match.GetIndex(),
		Cursor:      search.NewCursor(blk.Num(), blk.ID(), match.TransactionIDPrefix()),
		IrrBlockNum: irrBlockNum,
		BlockNum:    blk.Num(),
		Undo:        undo,
	}

	err := match.FillProtoSpecific(pbMatch, blk)
	if err != nil {
		return nil, err
	}

	return pbMatch, nil
}

func liveMarkerToProto(blk *bstream.Block, irrBlockNum uint64, match search.SearchMatch) (*pb.SearchMatch, error) {
	pbMatch := &pb.SearchMatch{
		Cursor:      search.NewCursor(blk.Num(), blk.ID(), ""),
		IrrBlockNum: irrBlockNum,
		BlockNum:    blk.Num(),
	}

	err := match.FillProtoSpecific(pbMatch, blk)
	if err != nil {
		return nil, err
	}

	return pbMatch, nil
}
