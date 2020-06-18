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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch"
	"github.com/dfuse-io/bstream"
	"go.uber.org/zap"
)

type SingleIndex struct {
	index.Index
	blockID string
	lock    sync.RWMutex
	path    string
}

func (i *SingleIndex) GetIndex() index.Index {
	return i.Index
}

func (i *SingleIndex) Delete() {
	if err := i.Index.Close(); err != nil {
		zlog.Warn("error closing index, chances it'll leak!", zap.Error(err))
	}
	if err := os.RemoveAll(i.path); err != nil {
		zlog.Warn("error removing index, watch your disk", zap.Error(err))
	}
}

//PreIndexer is a bstream Preprocessor that returns the bleve object instead from a bstream.block
type PreIndexer struct {
	mapper          BlockMapper
	liveIndexesPath string
}

func NewPreIndexer(blockMapper BlockMapper, liveIndexesPath string) *PreIndexer {
	if err := blockMapper.Validate(); err != nil {
		zlog.Panic(err.Error())
	}

	return &PreIndexer{
		mapper:          blockMapper,
		liveIndexesPath: liveIndexesPath,
	}
}

func (i *PreIndexer) Preprocess(blk *bstream.Block) (interface{}, error) {
	docsList, err := i.mapper.MapToBleve(blk)
	if err != nil {
		return nil, err
	}

	idx, analysisQueue, err := i.openLiveIndex(blk.Num(), blk.ID())
	if err != nil {
		return nil, err
	}

	batch := index.NewBatch()
	for _, doc := range docsList {
		batch.Update(doc)
	}
	err = idx.Batch(batch)
	if err != nil {
		analysisQueue.Close()
		return nil, err
	}

	analysisQueue.Close()
	return idx, nil

}

func (i *PreIndexer) openLiveIndex(blockNum uint64, blockID string) (*SingleIndex, *index.AnalysisQueue, error) {
	path := fmt.Sprintf(filepath.Join(i.liveIndexesPath, "%d-%s-%d.bleve"), blockNum, blockID, time.Now().UnixNano())
	analysisQueue := index.NewAnalysisQueue(1)
	idxer, err := scorch.NewScorch("data", map[string]interface{}{
		"forceSegmentType":    "zap",
		"forceSegmentVersion": 14,
		"path":                path,
		"unsafe_batch":        true,
	}, analysisQueue)

	if err != nil {
		return nil, nil, fmt.Errorf("creating ramdisk-based scorch index: %s", err)
	}

	err = idxer.Open()
	if err != nil {
		return nil, nil, fmt.Errorf("opening ramdisk-based scorch index: %s", err)
	}

	idx := &SingleIndex{
		Index:   idxer,
		blockID: blockID,
		path:    path,
	}
	return idx, analysisQueue, nil
}
