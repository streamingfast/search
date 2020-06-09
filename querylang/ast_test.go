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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToBleveQuery(t *testing.T) {
	tests := []struct {
		in          string
		transformer FieldTransformer
		expectBleve string
	}{
		{
			in:          "account:eoscanadacom",
			expectBleve: `{"term":"eoscanadacom","field":"account"}`,
		},
		{
			in:          "data.active:true",
			expectBleve: `{"bool":true,"field":"data.active"}`,
		},
		{
			in:          "data.active:false",
			expectBleve: `{"bool":false,"field":"data.active"}`,
		},
		{
			in:          `data.active:"true"`,
			expectBleve: `{"term":"true","field":"data.active"}`,
		},
		{
			in:          "receiver:eoscanadacom account:eoscanadacom",
			expectBleve: `{"conjuncts":[{"term":"eoscanadacom","field":"account"},{"term":"eoscanadacom","field":"receiver"}]}`,
		},
		{
			in:          "account:eoscanadacom receiver:eoscanadacom",
			expectBleve: `{"conjuncts":[{"term":"eoscanadacom","field":"account"},{"term":"eoscanadacom","field":"receiver"}]}`,
		},
		{
			in:          "receiver:eoscanadacom (action:transfer OR action:issue)",
			expectBleve: `{"conjuncts":[{"disjuncts":[{"term":"issue","field":"action"},{"term":"transfer","field":"action"}],"min":1},{"term":"eoscanadacom","field":"receiver"}]}`,
		},
		{
			in:          "receiver:eoscanadacom -(action:transfer OR action:issue)",
			expectBleve: `{"conjuncts":[{"must_not":{"disjuncts":[{"disjuncts":[{"term":"issue","field":"action"},{"term":"transfer","field":"action"}],"min":1}],"min":0}},{"term":"eoscanadacom","field":"receiver"}]}`,
		},
		{
			in:          "-receiver:eoscanadacom (action:transfer OR action:issue)",
			expectBleve: `{"conjuncts":[{"disjuncts":[{"term":"issue","field":"action"},{"term":"transfer","field":"action"}],"min":1},{"must_not":{"disjuncts":[{"term":"eoscanadacom","field":"receiver"}],"min":0}}]}`,
		},
		{
			in: "receiver:eoscanadacom (action:transfer OR action:issue) account:eoscanadacom (data.from:eoscanadacom OR data.to:eoscanadacom)",
			expectBleve: `{"conjuncts":[
		    {"term":"eoscanadacom","field":"account"},
		    {"disjuncts":[{"term":"issue","field":"action"},
		                  {"term":"transfer","field":"action"}],
		     "min":1},
		    {"disjuncts":[{"term":"eoscanadacom","field":"data.from"},
		                  {"term":"eoscanadacom","field":"data.to"}],
		     "min":1},
		    {"term":"eoscanadacom","field":"receiver"}
			]
		}`,
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			ast, err := Parse(test.in)
			require.NoError(t, err)

			res := ast.ToBleve()

			cnt, err := json.Marshal(res)
			require.NoError(t, err)
			assert.JSONEq(t, test.expectBleve, string(cnt), string(cnt))
		})
	}
}

func TestFindAllFieldNames(t *testing.T) {
	tests := []struct {
		in                 string
		expectedFieldNames []string
	}{
		{
			"account:eoscanadacom",
			[]string{"account"},
		},
		{
			"data.active:true",
			[]string{"data.active"},
		},
		{
			"data.active:false",
			[]string{"data.active"},
		},
		{
			`data.active:"true"`,
			[]string{"data.active"},
		},
		{
			"receiver:eoscanadacom account:eoscanadacom",
			[]string{"receiver", "account"},
		},
		{
			"receiver:eoscanadacom (action:transfer OR action:issue)",
			[]string{"receiver", "action"},
		},
		{
			"receiver:eoscanadacom (action:transfer OR action:issue) account:eoscanadacom (data.from:eoscanadacom OR data.to:eoscanadacom)",
			[]string{"receiver", "action", "account", "data.from", "data.to"},
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			ast, err := Parse(test.in)
			require.NoError(t, err)

			actuals := ast.FindAllFieldNames()
			assert.ElementsMatch(t, test.expectedFieldNames, actuals)
		})
	}
}

func TestEmptyQueries(t *testing.T) {
	tests := []struct {
		in             string
		expectedOutput string
	}{
		{
			"-action:patate",
			`{"ands":[{"and":{"minus":"-","name":"action","str":"patate"}}]}`,
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			ast, err := Parse(test.in)
			require.NoError(t, err)
			cnt, err := json.Marshal(ast)
			require.NoError(t, err)
			assert.Equal(t, test.expectedOutput, string(cnt), string(cnt))
		})
	}
}

func TestPurgeDeprecatedStatus(t *testing.T) {
	tests := []struct {
		in             string
		expectedOutput string
		expectedError  error
	}{
		{
			"account:bob",
			`{"ands":[{"and":{"name":"account","str":"bob"}}]}`,
			nil,
		},
		{
			"status:executed",
			``,
			fmt.Errorf("empty query after deprecated 'status' field removal"),
		},
		{
			"account:bob status:executed",
			`{"ands":[{"and":{"name":"account","str":"bob"}}]}`,
			nil,
		},
		{
			"account:bob -status:executed",
			`{"ands":[{"and":{"name":"account","str":"bob"}}]}`,
			fmt.Errorf("negated 'status' deprecated"),
		},
		{
			"account:bob status:soft_fail",
			``,
			fmt.Errorf("'status' other than 'executed' is invalid"),
		},
		{
			"(account:bob OR status:executed)",
			``,
			fmt.Errorf("'status' field invalid in OR clause"),
		},
		{
			"(account:bob OR status:executed OR status:executed)",
			``,
			fmt.Errorf("'status' field invalid in OR clause"),
		},
		{
			"(account:bob OR status:soft_fail)",
			``,
			fmt.Errorf("'status' field invalid in OR clause"),
		},
		{
			"-(account:bob OR status:soft_fail)",
			``,
			fmt.Errorf("'status' field invalid in OR clause"),
		},
		{
			"-(account:bob OR account:mama)",
			`{"ands":[{"minus":"-","ors":[{"name":"account","str":"bob"},{"name":"account","str":"mama"}]}]}`,
			nil,
		},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("index %d", idx+1), func(t *testing.T) {
			ast, err := Parse(test.in)
			require.NoError(t, err)

			err = ast.PurgeDeprecatedStatusField()
			if test.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, test.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
				cnt, err := json.Marshal(ast)
				require.NoError(t, err)
				assert.Equal(t, test.expectedOutput, string(cnt), string(cnt))
			}
		})
	}
}
