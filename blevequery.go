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
	"crypto/md5"
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/search/query"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/search/querylang"
	"google.golang.org/grpc/codes"
)

type BleveQueryFactory func(rawQuery string) *BleveQuery

type BleveQuery struct {
	Raw              string
	ast              *querylang.AST
	FieldTransformer querylang.FieldTransformer
	query            query.Query
	FieldNames       []string
	Validator        BleveQueryValdiator
}

func NewParsedQuery(rawQuery string) (*BleveQuery, error) {
	bquery := GetBleveQueryFactory(rawQuery)

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

func (q *BleveQuery) Hash() (string, error) {
	astJSON, err := json.Marshal(q.ast)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", md5.Sum(astJSON)), nil
}

func (q *BleveQuery) Parse() error {
	query, err := querylang.Parse(q.Raw)
	if err != nil {
		return err
	}

	q.ast = query

	if err := query.PurgeDeprecatedStatusField(); err != nil {
		return fmt.Errorf("'status' field deprecated, only 'executed' actions are indexed nowadays (%s); see release notes", err)
	}

	if err := query.ApplyTransforms(q.FieldTransformer); err != nil {
		return fmt.Errorf("applying transforms: %s", err)
	}

	q.FieldNames = query.FindAllFieldNames()
	q.query = query.ToBleve()

	return nil
}

func (q *BleveQuery) BleveQuery() query.Query {
	return q.query
}

func (q *BleveQuery) Validate() error {
	if q.Validator == nil {
		return nil
	}

	return q.Validator.Validate(q)
}

type BleveQueryValdiator interface {
	Validate(q *BleveQuery) error
}
