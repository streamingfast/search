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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/search/query"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/search/sqe"
	"google.golang.org/grpc/codes"
)

type BleveQueryFactory func(rawQuery string) *BleveQuery

type BleveQuery struct {
	Raw              string
	ast              sqe.Expression
	FieldTransformer sqe.FieldTransformer
	query            query.Query
	FieldNames       []string
	Validator        BleveQueryValidator
}

func NewParsedQuery(ctx context.Context, rawQuery string) (*BleveQuery, error) {
	// TODO: move this where it belongs?  This infers a global which
	// is annoying (GetBleveQueryFactory)
	bquery := GetBleveQueryFactory(rawQuery)

	if err := bquery.Parse(ctx); err != nil {
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

	hash := md5.Sum(astJSON)

	return hex.EncodeToString(hash[:]), nil
}

func (q *BleveQuery) Parse(ctx context.Context) error {
	expr, err := sqe.Parse(ctx, q.Raw)
	if err != nil {
		return err
	}

	q.ast = expr

	// FIXME: this should belong to the ApplyTransforms and only be true in EOSIO land.
	// NOTE TO SELF: perhaps we simply remove it today.. it was for transitioning.
	// if err := query.PurgeDeprecatedStatusField(); err != nil {
	// 	return fmt.Errorf("'status' field deprecated, only 'executed' actions are indexed nowadays (%s); see release notes", err)
	// }

	if err := sqe.TransformExpression(expr, q.FieldTransformer); err != nil {
		return fmt.Errorf("applying transforms: %s", err)
	}

	q.FieldNames = sqe.ExtractAllFieldNames(expr)
	q.query = sqe.ExpressionToBleve(expr)

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

type BleveQueryValidator interface {
	Validate(q *BleveQuery) error
}
