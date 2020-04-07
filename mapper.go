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
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/document"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/mapping"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/dfuse-io/bstream"
)

type Mapper struct {
	mapper      *mapping.IndexMappingImpl
	blockMapper BlockMapper
}

type BlockMapper interface {
	Map(mapper *mapping.IndexMappingImpl, block *bstream.Block) ([]*document.Document, error)
	IndexMapping() *mapping.IndexMappingImpl
}

const TimeFormatBleveID = "2006-01-02T15-04-05.000"

func NewMapper(blockMapper BlockMapper) (*Mapper, error) {
	m := blockMapper.IndexMapping()

	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("validation of our mapper failed: %s", err)
	}

	return &Mapper{mapper: m, blockMapper: blockMapper}, nil
}

func (m *Mapper) PreprocessBlock(blk *bstream.Block) (interface{}, error) {
	return m.MapBlock(blk)
}

func (m *Mapper) MapBlock(blk *bstream.Block) ([]*document.Document, error) {
	batch, err := m.blockMapper.Map(m.mapper, blk)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

func trimZeroPrefix(in string) string {
	out := strings.TrimLeft(in, "0")
	if len(out) == 0 {
		return "0"
	}
	return out
}

// FIXME: This sucks a little bit because we need to pass the set of parameters that is the
//        union of all block mapper constructor parameters for all supported protocol type.
//        Not a too big deal now, since there is only one total, if this grows too much, refactoring
//        would make sense.
func MustGetBlockMapper(protocol pbbstream.Protocol, hooksActionName string, restrictions []*Restriction) BlockMapper {
	var mapper BlockMapper
	bstream.MustDoForProtocol(protocol, map[pbbstream.Protocol]func(){
		pbbstream.Protocol_EOS: func() { mapper = NewEOSBlockMapper(hooksActionName, restrictions) },
		// pbbstream.Protocol_ETH: func() { mapper = NewETHBlockMapper() },
	})

	return mapper
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
var disabledMapping *mapping.DocumentMapping
var txtFieldMapping *mapping.FieldMapping
var boolFieldMapping *mapping.FieldMapping
var sortableNumericFieldMapping *mapping.FieldMapping
var dynamicNestedDocMapping *mapping.DocumentMapping

func init() {
	disabledMapping = bleve.NewDocumentDisabledMapping()

	sortableNumericFieldMapping = bleve.NewNumericFieldMapping()
	sortableNumericFieldMapping.Index = true
	sortableNumericFieldMapping.Store = false
	sortableNumericFieldMapping.IncludeInAll = false
	sortableNumericFieldMapping.DocValues = true // required for sorting on that field

	boolFieldMapping = bleve.NewBooleanFieldMapping()
	boolFieldMapping.Index = true               // index this field
	boolFieldMapping.Store = false              // save space, do not store value
	boolFieldMapping.DocValues = false          // save space, cannot sort or build facet on this field
	boolFieldMapping.Analyzer = keyword.Name    // ensure keyword analyzer
	boolFieldMapping.IncludeInAll = false       // you have _all field disabled
	boolFieldMapping.IncludeTermVectors = false // save space, cannot do phrase search or result highlighting

	txtFieldMapping = bleve.NewTextFieldMapping()
	txtFieldMapping.Index = true               // index this field
	txtFieldMapping.Store = false              // save space, do not store value
	txtFieldMapping.DocValues = false          // save space, cannot sort or build facet on this field
	txtFieldMapping.Analyzer = keyword.Name    // ensure keyword analyzer
	txtFieldMapping.IncludeInAll = false       // you have _all field disabled
	txtFieldMapping.IncludeTermVectors = false // save space, cannot do phrase search or result highlighting

	// a reusable nested doc with dynamic mapping (should be fine for db.*.*, but event.* might be a pandora box)
	dynamicNestedDocMapping = bleve.NewDocumentMapping()
	dynamicNestedDocMapping.Dynamic = true
	dynamicNestedDocMapping.DefaultAnalyzer = keyword.Name
}
