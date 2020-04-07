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

package roarcache

import (
	"github.com/RoaringBitmap/roaring"
)

type Local struct {
	items map[string]*roaring.Bitmap

	//shardSize uint64
	// TTL for the keys in memcache in seconds
	//ttlSeconds int32
}

func NewLocal() Cache {
	return &Local{
		items: map[string]*roaring.Bitmap{},
		//shardSize:  0,
		//ttlSeconds: 0,
	}
}

func (l Local) Put(key string, roar *roaring.Bitmap) error {
	panic("implement me")
}

func (l Local) Get(key string, roar *roaring.Bitmap) error {
	panic("implement me")
}
