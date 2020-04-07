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

	"github.com/alecthomas/participle/lexer"
	"github.com/andreyvit/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestASTParser(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  string
		err  error
	}{
		{
			name: "single quote works",
			in:   `from:'transfer(address,uint256)'`,
			out:  `{"ands":[{"and":{"name":"from","qstr":"transfer(address,uint256)"}}]}`,
		},
		{
			name: "mixed quote start-end errors out",
			in:   `from:"transfer(address,uint256)'`,
			err:  &lexer.Error{Message: "unexpected \")\" (expected <colon>)", Pos: lexer.Position{Offset: 30, Line: 1, Column: 31}},
		},
		{
			name: "mixed quote end-start errors out",
			in:   `from:'transfer(address,uint256)"`,
			err:  &lexer.Error{Message: "unexpected \")\" (expected <colon>)", Pos: lexer.Position{Offset: 30, Line: 1, Column: 31}},
		},
		{
			name: "double quote works",
			in:   `from:"transfer(address,uint256)"`,
			out:  `{"ands":[{"and":{"name":"from","qstr":"transfer(address,uint256)"}}]}`,
		},
		{
			in:  `data.from:"eoscanadacom" (action:transfer OR action:issue OR action:matant) data.to:eoscanadacom data.mama:eoscarotte`,
			out: `{"ands":[{"and":{"name":"data.from","qstr":"eoscanadacom"}},{"ors":[{"name":"action","str":"transfer"},{"name":"action","str":"issue"},{"name":"action","str":"matant"}]},{"and":{"name":"data.to","str":"eoscanadacom"}},{"and":{"name":"data.mama","str":"eoscarotte"}}]}`,
		},
		{
			in:  "(data.from:eoscanadacom OR data.to:eoscanadacom)",
			out: `{"ands":[{"ors":[{"name":"data.from","str":"eoscanadacom"},{"name":"data.to","str":"eoscanadacom"}]}]}`,
		},
		{
			in:  "(-data.from:eoscanadacom OR -data.to:eoscanadacom)",
			out: `{"ands":[{"ors":[{"minus":"-","name":"data.from","str":"eoscanadacom"},{"minus":"-","name":"data.to","str":"eoscanadacom"}]}]}`,
		},
		{
			in:  "-(data.from:eoscanadacom OR data.to:eoscanadacom)",
			out: `{"ands":[{"minus":"-","ors":[{"name":"data.from","str":"eoscanadacom"},{"name":"data.to","str":"eoscanadacom"}]}]}`,
		},
		{
			in:  "-data.from:eoscanadacom",
			out: `{"ands":[{"and":{"minus":"-","name":"data.from","str":"eoscanadacom"}}]}`,
		},
		{
			in:  "account:hello receiver:world",
			out: `{"ands":[{"and":{"name":"account","str":"hello"}},{"and":{"name":"receiver","str":"world"}}]}`,
		},
		{
			in:  "account:hello (receiver:world OR action:transfer)",
			out: `{"ands":[{"and":{"name":"account","str":"hello"}},{"ors":[{"name":"receiver","str":"world"},{"name":"action","str":"transfer"}]}]}`,
		},
		{
			in:  "account:hello (account:hello OR account:world)",
			out: `{"ands":[{"and":{"name":"account","str":"hello"}},{"ors":[{"name":"account","str":"hello"},{"name":"account","str":"world"}]}]}`,
		},
		{
			in:  `account:hello data.quantity:"60.0000 CET"`,
			out: `{"ands":[{"and":{"name":"account","str":"hello"}},{"and":{"name":"data.quantity","qstr":"60.0000 CET"}}]}`,
		},
		{
			in:  `data.active:true`,
			out: `{"ands":[{"and":{"name":"data.active","str":"true"}}]}`,
		},
		{
			in:  `account:eosio.msig action:exec data.proposal_name:unregupdate`,
			out: `{"ands":[{"and":{"name":"account","str":"eosio.msig"}},{"and":{"name":"action","str":"exec"}},{"and":{"name":"data.proposal_name","str":"unregupdate"}}]}`,
		},
	}

	for idx, test := range tests {
		name := test.name
		if name == "" {
			name = fmt.Sprintf("index %d", idx+1)
		}

		t.Run(name, func(t *testing.T) {
			expr, err := Parse(test.in)
			require.Equal(t, test.err, err)
			if test.err != nil {
				return
			}

			outValue := map[string]interface{}{}
			json.Unmarshal([]byte(test.out), &outValue)

			cnt, err := json.Marshal(expr)
			assert.NoError(t, err)

			cntJSON, err := json.MarshalIndent(expr, "", "  ")
			assert.NoError(t, err)

			outJSON, err := json.MarshalIndent(outValue, "", "  ")
			assert.NoError(t, err)

			assert.JSONEq(t, string(outJSON), string(cntJSON), diff.LineDiff(string(outJSON), string(cntJSON))+"\n\nActual:\n"+string(cnt))
		})
	}
}
