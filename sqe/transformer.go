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

package sqe

type FieldTransformer interface {
	Transform(field *SearchTerm) error
}

type noOpTransformer struct{}

func (noOpTransformer) Transform(field *SearchTerm) error {
	return nil
}

var NoOpFieldTransformer noOpTransformer
