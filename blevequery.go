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
	"sort"
	"strings"

	"github.com/blevesearch/bleve/search/query"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/search/querylang"
	"google.golang.org/grpc/codes"
)

type BleveQuery struct {
	Raw              string
	ast              *querylang.AST
	fieldTransformer querylang.FieldTransformer
	query            query.Query
	fieldNames       []string
	validator        BleveQueryValdiator
}

func NewParsedQuery(protocol pbbstream.Protocol, rawQuery string) (*BleveQuery, error) {
	bquery := &BleveQuery{
		Raw: rawQuery,
	}

	bstream.MustDoForProtocol(protocol, map[pbbstream.Protocol]func(){
		// pbbstream.Protocol_ETH: bquery.InitializeForETH,
		pbbstream.Protocol_EOS: bquery.InitializeForEOS,
	})

	if err := bquery.Parse(); err != nil {
		// FIXME: when the query failed, we want to return
		// like in the REST call, but that one needs to wrap
		// all the status.New()` with details. AppInvalidQueryError(ctx, err, bquery.Raw)
		return nil, derr.Statusf(codes.InvalidArgument, "invalid query: %s (query: %q)", err.Error(), rawQuery)
	}

	if err := bquery.Validate(); err != nil {
		// FIXME: how will that bubble back, with the right error code on the REST front-end, so we have the level of details we used to have.
		return nil, err
	}

	return bquery, nil
}

func (q *BleveQuery) InitializeForEOS() {
	q.fieldTransformer = querylang.NoOpFieldTransformer
	q.validator = &EOSBleveQueryValidator{}
}

// func (q *BleveQuery) InitializeForETH() {
// 	q.fieldTransformer = &querylang.ETHFieldTransformer{}
// 	q.validator = &ETHBleveQueryValidator{}
// }

func (q *BleveQuery) Parse() error {
	query, err := querylang.Parse(q.Raw)
	if err != nil {
		return err
	}

	q.ast = query

	if err := query.PurgeDeprecatedStatusField(); err != nil {
		return fmt.Errorf("'status' field deprecated, only 'executed' actions are indexed nowadays (%s); see release notes", err)
	}

	if err := query.ApplyTransforms(q.fieldTransformer); err != nil {
		return fmt.Errorf("applying transforms: %s", err)
	}

	q.fieldNames = query.FindAllFieldNames()
	q.query = query.ToBleve()

	return nil
}

func (q *BleveQuery) BleveQuery() query.Query {
	return q.query
}

func (q *BleveQuery) Validate() error {
	if q.validator == nil {
		return nil
	}

	return q.validator.Validate(q)
}

type BleveQueryValdiator interface {
	Validate(q *BleveQuery) error
}

var NoOpBleveQueryValidator BleveQueryValdiator

type EOSBleveQueryValidator struct{}

func (v *EOSBleveQueryValidator) Validate(q *BleveQuery) error {
	indexedFieldsMap := GetEOSIndexedFieldsMap()

	var unknownFields []string
	for _, fieldName := range q.fieldNames {
		if strings.HasPrefix(fieldName, "data.") {
			fieldName = strings.Join(strings.Split(fieldName, ".")[:2], ".")
		}

		if indexedFieldsMap[fieldName] != nil || strings.HasPrefix(fieldName, "event.") || strings.HasPrefix(fieldName, "parent.") /* we could list the optional fields for `parent.*` */ {
			continue
		}
		unknownFields = append(unknownFields, fieldName)
	}

	if len(unknownFields) <= 0 {
		return nil
	}

	sort.Strings(unknownFields)

	invalidArgString := "The following fields you are trying to search are not currently indexed: '%s'. Contact our support team for more."
	return derr.Statusf(codes.InvalidArgument, invalidArgString, strings.Join(unknownFields, "', '"))
}
