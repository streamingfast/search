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
	"regexp"
	"strings"

	"golang.org/x/crypto/sha3"
)

var methodHexRegex = regexp.MustCompile("^[0-9a-f]{8}$")

type FieldTransformer interface {
	Transform(field *Field) error
}

var NoOpFieldTransformer FieldTransformer

func trimZeroX(in string) string {
	if strings.HasPrefix(in, "0x") {
		return in[2:]
	}
	return in
}

func trimZeroPrefix(in string) string {
	out := strings.TrimLeft(in, "0")
	if len(out) == 0 {
		return "0"
	}
	return out
}

// Keccak256 calculates and returns the Keccak256 hash of the input data.
func Keccak256(data []byte) []byte {
	d := sha3.NewLegacyKeccak256()
	d.Write(data)

	return d.Sum(nil)
}
