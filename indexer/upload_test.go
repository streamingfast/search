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

package indexer

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/dfuse-io/dstore"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_Upload(t *testing.T) {
	fileBody := "this archive contains some test files."
	pipe := &Pipeline{
		indexesStore: dstore.NewMockStore(func(base string, f io.Reader) (err error) {
			if _, err := io.Copy(os.Stdout, f); err != nil {
				require.NoError(t, err)
			}
			return context.Canceled
		}),
	}

	walkIndexfileFunc = func(indexPath string, tw *tar.Writer) (err error) {
		hdr := &tar.Header{
			Name: "shards-0/0000000000.bleve",
			Mode: 0600,
			Size: int64(len(fileBody)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			require.NoError(t, err)
		}
		if _, err := tw.Write([]byte(fileBody)); err != nil {
			require.NoError(t, err)
		}
		return nil
	}

	err := pipe.Upload(0, "")
	assert.Equal(t, fmt.Errorf("writing to google storage: context canceled"), err)
}
