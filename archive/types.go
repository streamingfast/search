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
	pb "github.com/streamingfast/pbgo/sf/search/v1"
	"github.com/streamingfast/search"
)

type incomingResult struct {
	indexStartBlock uint64 // For debug display only
	indexEndBlock   uint64

	resultChan chan *singleIndexResult
}

func archiveSearchMatchToProto(match search.SearchMatch) (*pb.SearchMatch, error) {
	pbMatch := &pb.SearchMatch{
		TrxIdPrefix: match.TransactionIDPrefix(),
		Cursor:      search.NewCursor(match.BlockNum(), "", match.TransactionIDPrefix()),
		IrrBlockNum: match.BlockNum(),
		BlockNum:    match.BlockNum(),
	}

	err := match.FillProtoSpecific(pbMatch, nil)
	if err != nil {
		return nil, err
	}

	return pbMatch, nil
}
