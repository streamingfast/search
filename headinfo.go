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

package search

import (
	"context"

	"github.com/dfuse-io/bstream"
	pbheadinfo "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	"go.uber.org/zap"
)

func GetLibInfo(headinfoCli pbheadinfo.HeadInfoClient) (bstream.BlockRef, error) {
	headResp, err := headinfoCli.GetHeadInfo(context.Background(), &pbheadinfo.HeadInfoRequest{
		Source: pbheadinfo.HeadInfoRequest_NETWORK,
	})
	if err != nil {
		return nil, err
	}
	zlog.Debug("head info response",
		zap.Uint64("lib_num", headResp.LibNum),
		zap.String("lib_id", headResp.LibID),
		zap.Uint64("head_num", headResp.HeadNum),
		zap.String("head_id", headResp.HeadID),
		zap.String("head_time", headResp.HeadTime.String()))

	return bstream.NewBlockRef(headResp.LibID, headResp.LibNum), nil
}
