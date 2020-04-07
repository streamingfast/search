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
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	mcache "github.com/bradfitz/gomemcache/memcache"
)

type Cache interface {
	Put(key string, roar *roaring.Bitmap) error
	Get(key string, roar *roaring.Bitmap) error
}

type Memcache struct {
	client *mcache.Client

	shardSize uint64
	// TTL for the keys in memcache in seconds
	ttlSeconds int32
}

func NewMemcache(serverAddr string, ttl time.Duration, shardSize uint64) *Memcache {
	client := mcache.New(serverAddr)
	client.Timeout = 500 * time.Millisecond
	client.MaxIdleConns = 60
	return &Memcache{
		client:     client,
		ttlSeconds: int32(ttl.Seconds()),
		shardSize:  shardSize,
	}
}

// StoreBitmap assumes it has full control on the `roar` now. Thread safe otherwise.
func (c *Memcache) Put(key string, roar *roaring.Bitmap) error {
	cnt, err := roar.ToBytes()
	if err != nil {
		return err
	}

	err = c.client.Set(&mcache.Item{
		Key:        c.computeKey(key),
		Value:      cnt,
		Expiration: c.ttlSeconds,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Memcache) Get(key string, roar *roaring.Bitmap) error {
	item, err := c.client.Get(c.computeKey(key))
	if err != nil {
		return err
	}

	if _, err = roar.FromBuffer(item.Value); err != nil {
		return err
	}

	return nil
}

func (c *Memcache) computeKey(key string) string {
	return fmt.Sprintf("rc:%d:%s", c.shardSize, key)
}
