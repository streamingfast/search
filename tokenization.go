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
	"crypto/sha256"
	"encoding/hex"
	"go.uber.org/zap"
	"net/url"
	"sort"
	"strings"
)

type ValueType int32

const (
	accountType ValueType = iota
	addressType
	actionType
	actionIndexType
	assetType
	booleanType
	blockNumType
	hexType
	freeFormType
	nameType
	permissionType
	transactionIDType
	numberType
)

type IndexedField struct {
	Name      string    `json:"name"`
	ValueType ValueType `json:"type"`
}

func toList(in map[string]bool) (out []string) {
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)
	return
}

func tokenizeEvent(key string, data string) url.Values {
	out, err := url.ParseQuery(data)
	if err != nil {
		zlog.Debug("error parsing dfuseiohooks", zap.Error(err))
		return nil
	}

	var authKey bool

	for k, vals := range out {
		// 16 chars keys for everyone
		if len(k) > 16 {
			zlog.Debug("dfuse hooks event field name too long", zap.String("field_prefix", k[:16]))
			return nil
		}

		if !authKey {
			// For free keys, limit to 64 chars chars the key
			for _, v := range vals {
				if len(v) > 64 {
					zlog.Debug("dfuse hooks event field value too long", zap.String("field", k), zap.Int("value_size", len(v)))
					return nil
				}
			}
		}
	}

	if !authKey && len(out) > 3 {
		zlog.Debug("dfuse hooks event has more than 3 fields", zap.Int("field_count", len(out)))
		return nil
	}

	return out
}

func hashKeys(in, out map[string]interface{}, fields []IndexedField) {
	for _, field := range fields {
		f, found := in[field.Name]
		if !found {
			continue
		}

		val, ok := f.(string)
		if !ok {
			continue
		}

		res, err := hex.DecodeString(val)
		if err != nil {
			continue
		}

		h := sha256.New()
		_, _ = h.Write(res)
		out[field.Name] = hex.EncodeToString(h.Sum(nil))
	}
}

func cleanString(input string) (output string) {
	return strings.ToLower(input)
}

func cleanIdentifier(input string) (output string) {
	return strings.ToLower(strings.Map(identifierChars, input))
}

func identifierChars(r rune) rune {
	switch {
	case r >= 'a' && r <= 'z':
		return r
	case r >= 'A' && r <= 'Z':
		return r
	case r >= '0' && r <= '9':
		return r
	case r == '_':
		return r
	}
	return -1
}

func chunkString(input string) (out []string) {
	out = strings.Split(strings.ToLower(input), " ")

	for _, sep := range []string{"!", "-", "@", "|", "$", "%", "^"} {
		var newOut []string
		for _, chunk := range out {
			newOut = append(newOut, strings.Split(chunk, sep)...)
		}
		out = newOut
	}

	return out
}
