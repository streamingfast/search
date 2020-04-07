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

package querylang

import (
	"fmt"

	"github.com/blevesearch/bleve/search/query"
)

type AST struct {
	AndExpr []*SubGroup `parser:"@@ { @@ }" json:"ands"`
}

type SubGroup struct {
	Minus    string   `parser:"( @Minus?" json:"minus,omitempty"`
	OrFields []*Field `parser:"  LeftParenthesis @@ ( OrOperator @@ )+ RightParenthesis" json:"ors,omitempty"`
	AndField *Field   `parser:"  | @@ )" json:"and,omitempty"`
}

type Field struct {
	Minus        string `parser:"@Minus?" json:"minus,omitempty"`
	Name         string `parser:"@Name Colon" json:"name"`
	QuotedString string `parser:"(  @QuotedString" json:"qstr,omitempty"`
	String       string `parser:" | @Name )" json:"str,omitempty"`
}

func (a *AST) ApplyTransforms(transformer FieldTransformer) error {
	if transformer == nil {
		return nil
	}

	for _, sub := range a.AndExpr {
		if sub.AndField != nil {
			if err := transformer.Transform(sub.AndField); err != nil {
				return fmt.Errorf("field %q: %s", sub.AndField.Name, err)
			}
		}
		for _, orField := range sub.OrFields {
			if err := transformer.Transform(orField); err != nil {
				return fmt.Errorf("field %q: %s", orField.Name, err)
			}
		}
	}
	return nil
}

func (a *AST) ToBleve() query.Query {
	if len(a.AndExpr) == 1 && a.AndExpr[0].AndField != nil {
		return a.AndExpr[0].AndField.ToQuery()
	}

	out := query.NewConjunctionQuery(nil)
	for _, expr := range a.AndExpr {
		if expr.AndField != nil {
			fieldQuery := expr.AndField.ToQuery()
			out.AddQuery(fieldQuery)
		} else {
			disjunct := query.NewDisjunctionQuery(nil)
			disjunct.SetMin(1)
			for _, field := range expr.OrFields {
				fieldQuery := field.ToQuery()
				disjunct.AddQuery(fieldQuery)
			}

			if expr.Minus == "-" {
				boolQuery := query.NewBooleanQuery(nil, nil, []query.Query{disjunct})
				out.AddQuery(boolQuery)
			} else {
				out.AddQuery(disjunct)
			}
		}
	}

	return out
}

// FindAllFieldNames returns all used field names in the AST. There
// is **NO** ordering on the elements, i.e. they might not come in the
// same order specified in the AST.
func (a *AST) FindAllFieldNames() []string {
	fieldNamesMap := map[string]bool{}
	for _, expr := range a.AndExpr {
		for _, exprField := range expr.OrFields {
			fieldNamesMap[exprField.Name] = true
		}

		if expr.AndField != nil {
			fieldNamesMap[expr.AndField.Name] = true
		}
	}

	fieldNames := make([]string, 0, len(fieldNamesMap))
	for key := range fieldNamesMap {
		fieldNames = append(fieldNames, key)
	}

	return fieldNames
}

func (a *AST) PurgeDeprecatedStatusField() error {
	var newAndExpr []*SubGroup
	for _, expr := range a.AndExpr {

		if andField := expr.AndField; andField != nil {
			if andField.Name == "status" {
				status := andField.StringValue()
				if andField.Minus == "-" {
					return fmt.Errorf("negated 'status' deprecated")
				}
				if status != "executed" {
					return fmt.Errorf("'status' other than 'executed' is invalid")
				}
				// Backwards compatibility fix: Silently ignore the `status:executed`
				continue
			}
		} else {
			for _, andField := range expr.OrFields {
				if andField.Name == "status" {
					return fmt.Errorf("'status' field invalid in OR clause")
				}
			}
		}
		newAndExpr = append(newAndExpr, expr)
	}

	if newAndExpr == nil {
		return fmt.Errorf("empty query after deprecated 'status' field removal")
	}

	a.AndExpr = newAndExpr

	return nil
}

func (f *Field) Transform(transformer FieldTransformer) error {
	if transformer == nil {
		return nil
	}

	return transformer.Transform(f)
}

func (f *Field) ToQuery() query.Query {
	var fieldQuery query.FieldableQuery
	switch f.String {
	case "true":
		fieldQuery = query.NewBoolFieldQuery(true)
	case "false":
		fieldQuery = query.NewBoolFieldQuery(false)
	default:
		fieldQuery = query.NewTermQuery(f.StringValue())
	}

	fieldQuery.SetField(f.Name)
	if f.Minus == "-" {
		return query.NewBooleanQuery(nil, nil, []query.Query{fieldQuery})
	}

	return fieldQuery
}

func (f *Field) StringValue() string {
	// String takes precedence. If someone specified `""`, it means
	// the string value should be nil. Otherwise, a String should have
	// something in it..
	if f.String == "" {
		return f.QuotedString
	}

	return f.String
}

func (f *Field) SetString(in string) {
	f.QuotedString = in
	f.String = ""
}
