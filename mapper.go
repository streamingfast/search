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
	"strconv"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/mapping"
	"github.com/dfuse-io/bstream"
)

type BlockMapper interface {
	MapToBleve(block *bstream.Block) ([]*document.Document, error)
	Validate() error
}

const TimeFormatBleveID = "2006-01-02T15-04-05.000"

func AsPreprocessBlock(b BlockMapper) bstream.PreprocessFunc {
	return func(blk *bstream.Block) (interface{}, error) {
		batch, err := b.MapToBleve(blk)
		if err != nil {
			return nil, err
		}

		return batch, nil
	}
}

func trimZeroPrefix(in string) string {
	out := strings.TrimLeft(in, "0")
	if len(out) == 0 {
		return "0"
	}
	return out
}

func fromHexUint16(input string) (uint16, error) {
	val, err := strconv.ParseUint(input, 16, 16)
	if err != nil {
		return 0, err
	}
	return uint16(val), nil
}

func fromHexUint32(input string) (uint32, error) {
	val, err := strconv.ParseUint(input, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(val), nil
}

// General purpose mappers
var DisabledMapping *mapping.DocumentMapping
var TxtFieldMapping *mapping.FieldMapping
var BoolFieldMapping *mapping.FieldMapping
var SortableNumericFieldMapping *mapping.FieldMapping
var DynamicNestedDocMapping *mapping.DocumentMapping

func init() {
	DisabledMapping = bleve.NewDocumentDisabledMapping()

	SortableNumericFieldMapping = bleve.NewNumericFieldMapping()
	SortableNumericFieldMapping.Index = true
	SortableNumericFieldMapping.Store = false
	SortableNumericFieldMapping.IncludeInAll = false
	SortableNumericFieldMapping.DocValues = true // required for sorting on that field

	BoolFieldMapping = bleve.NewBooleanFieldMapping()
	BoolFieldMapping.Index = true               // index this field
	BoolFieldMapping.Store = false              // save space, do not store value
	BoolFieldMapping.DocValues = false          // save space, cannot sort or build facet on this field
	BoolFieldMapping.Analyzer = keyword.Name    // ensure keyword analyzer
	BoolFieldMapping.IncludeInAll = false       // you have _all field disabled
	BoolFieldMapping.IncludeTermVectors = false // save space, cannot do phrase search or result highlighting

	TxtFieldMapping = bleve.NewTextFieldMapping()
	TxtFieldMapping.Index = true               // index this field
	TxtFieldMapping.Store = false              // save space, do not store value
	TxtFieldMapping.DocValues = false          // save space, cannot sort or build facet on this field
	TxtFieldMapping.Analyzer = keyword.Name    // ensure keyword analyzer
	TxtFieldMapping.IncludeInAll = false       // you have _all field disabled
	TxtFieldMapping.IncludeTermVectors = false // save space, cannot do phrase search or result highlighting

	// a reusable nested doc with dynamic mapping (should be fine for db.*.*, but event.* might be a pandora box)
	DynamicNestedDocMapping = bleve.NewDocumentMapping()
	DynamicNestedDocMapping.Dynamic = true
	DynamicNestedDocMapping.DefaultAnalyzer = keyword.Name
}
