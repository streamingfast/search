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
	"encoding/json"
	"fmt"

	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	"github.com/eoscanada/eos-go"
)

var fixedEOSIndexedFields = []IndexedField{
	{"receiver", accountType},
	{"account", accountType},
	{"action", actionType},
	{"auth", permissionType},
	{"block_num", blockNumType},
	{"trx_idx", transactionIDType},
	{"scheduled", booleanType},
	{"status", freeFormType},
	{"notif", booleanType},
	{"input", booleanType},
	{"event", freeFormType},
}

var EOSIndexedFields = []IndexedField{
	{"account", accountType},
	{"active", freeFormType},
	{"active_key", freeFormType},
	{"actor", freeFormType},
	{"amount", assetType},
	{"auth", freeFormType},
	{"authority", freeFormType},
	{"bid", freeFormType},
	{"bidder", accountType},
	{"canceler", accountType},
	{"creator", accountType},
	{"executer", accountType},
	{"from", accountType},
	{"is_active", booleanType},
	{"is_priv", booleanType},
	{"isproxy", booleanType},
	{"issuer", accountType},
	{"level", freeFormType},
	{"location", freeFormType},
	{"maximum_supply", assetType},
	{"name", nameType},
	{"newname", nameType},
	{"owner", accountType},
	{"parent", accountType},
	{"payer", accountType},
	{"permission", permissionType},
	{"producer", accountType},
	{"producer_key", freeFormType},
	{"proposal_name", nameType},
	{"proposal_hash", freeFormType},
	{"proposer", accountType},
	{"proxy", freeFormType},
	{"public_key", freeFormType},
	{"producers", freeFormType},
	{"quant", freeFormType},
	{"quantity", freeFormType},
	{"ram_payer", accountType},
	{"receiver", accountType},
	{"requested", booleanType},
	{"requirement", freeFormType},
	{"symbol", freeFormType},
	{"threshold", freeFormType},
	{"to", accountType},
	{"transfer", freeFormType},
	{"voter", accountType},
	{"voter_name", nameType},
	{"weight", freeFormType},
}

//TODO: sha256 actual bytes (hex decode, etc.)
var hashedEOSDataIndexedFields = []IndexedField{
	{"abi", hexType},
	{"code", hexType}, // only for action = setcode
}

func tokenizeEOSExecutedAction(actTrace *pbdeos.ActionTrace) (out map[string]interface{}) {
	out = make(map[string]interface{})
	out["receiver"] = actTrace.Receipt.Receiver
	out["account"] = actTrace.Account()
	out["action"] = actTrace.Name()
	out["auth"] = tokenizeEOSAuthority(actTrace.Action.Authorization)
	out["data"] = tokenizeEOSDataObject(actTrace.Action.JsonData)

	return out
}

func tokenizeEOSAuthority(authorizations []*pbdeos.PermissionLevel) (out []string) {
	for _, auth := range authorizations {
		actor := auth.Actor
		perm := auth.Permission
		out = append(out, actor, fmt.Sprintf("%s@%s", actor, perm))
	}

	return
}

func tokenizeEOSDataObject(data string) map[string]interface{} {
	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(data), &jsonData); err != nil {
		return nil
	}

	out := make(map[string]interface{})
	for _, indexedField := range EOSIndexedFields {
		if value, exists := jsonData[indexedField.Name]; exists {
			out[indexedField.Name] = value
		}
	}

	hashKeys(jsonData, out, hashedEOSDataIndexedFields)

	// TODO: make sure we don't send strings that are more than 100 chars in the index..
	// some things put pixels in there.. if it matches the whitelist *bam* !

	return out
}

func isEOSValidName(input string) bool {
	val, _ := eos.StringToName(input)
	return eos.NameToString(val) == input
}

var cachedEOSIndexedFields []*IndexedField
var cachedEOSIndexedFieldsMap map[string]*IndexedField

// InitIndexedFields initialize the list of indexed fields of the service
func InitEOSIndexedFields() {
	fields := make([]*IndexedField, 0, len(fixedEOSIndexedFields)+len(EOSIndexedFields)+len(hashedEOSDataIndexedFields))

	for _, field := range fixedEOSIndexedFields {
		fields = append(fields, &IndexedField{field.Name, field.ValueType})
	}

	for _, field := range EOSIndexedFields {
		fields = append(fields, &IndexedField{"data." + field.Name, field.ValueType})
	}

	for _, field := range hashedEOSDataIndexedFields {
		fields = append(fields, &IndexedField{"data." + field.Name, field.ValueType})
	}

	fields = append(fields,
		&IndexedField{"ram.consumed", freeFormType},
		&IndexedField{"ram.released", freeFormType},
	)

	fields = append(fields,
		&IndexedField{"db.table", freeFormType},

		// Disabled so that if user complains, we can easily add it back. This should be
		// removed if we do not index `db.key` anymore.
		// &IndexedField{"db.key", freeFormType},
	)

	// Let's cache the fields so we do not re-compute them everytime.
	cachedEOSIndexedFields = fields

	// Let's compute the fields map from the actual fields slice
	cachedEOSIndexedFieldsMap = map[string]*IndexedField{}
	for _, field := range cachedEOSIndexedFields {
		cachedEOSIndexedFieldsMap[field.Name] = field
	}
}

// GetIndexedFields returns the list of indexed fields of the service, from the
// cached list of indexed fields. Function `InitIndexedFields` must be called prior
// using this function.
func GetEOSIndexedFields() []*IndexedField {
	if cachedEOSIndexedFields == nil {
		zlog.Panic("the indexed fields cache is nil, you must initialize it prior calling this method")
	}

	return cachedEOSIndexedFields
}

func GetEOSIndexedFieldsMap() map[string]*IndexedField {
	if cachedEOSIndexedFieldsMap == nil {
		zlog.Panic("the indexed fields map cache is nil, you must initialize it prior calling this method")
	}

	return cachedEOSIndexedFieldsMap
}
