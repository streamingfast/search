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
	"sort"

	"github.com/alecthomas/participle"
)

var parser = participle.MustBuild(
	&AST{},
	participle.Lexer(queryLexer),
	participle.Unquote("QuotedString"),
)

func Parse(input string) (expr *AST, err error) {
	expr = &AST{}
	err = parser.ParseString(input, expr)
	if err == nil {
		sortAST(expr)
	}
	return
}

func subgroupName(f *SubGroup) string {
	if f.AndField == nil {
		if len(f.OrFields) == 0 {
			return ""
		}
		return f.OrFields[0].Name
	}
	return f.AndField.Name
}

func subgroupValue(f *SubGroup) string {
	if f.AndField == nil {
		if len(f.OrFields) == 0 {
			return ""
		}
		return f.OrFields[0].StringValue()
	}
	return f.AndField.StringValue()
}

func sortAST(a *AST) {
	for _, ae := range a.AndExpr {
		if len(ae.OrFields) <= 1 {
			continue
		}
		sort.SliceStable(ae.OrFields, func(i, j int) bool {
			if ae.OrFields[i].Name == ae.OrFields[j].Name {
				return ae.OrFields[i].StringValue() < ae.OrFields[j].StringValue()
			}
			return ae.OrFields[i].Name < ae.OrFields[j].Name
		})
	}
	if len(a.AndExpr) <= 1 {
		return
	}
	sort.SliceStable(a.AndExpr, func(i, j int) bool {
		if subgroupName(a.AndExpr[i]) == subgroupName(a.AndExpr[j]) {
			return subgroupValue(a.AndExpr[i]) < subgroupValue(a.AndExpr[j])
		}
		return subgroupName(a.AndExpr[i]) < subgroupName(a.AndExpr[j])
	})
}
