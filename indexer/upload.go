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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.uber.org/zap"
)

func startBlockFromFileName(filename string) uint64 {
	startBlock, _ := strconv.ParseInt(filename, 10, 64)
	return uint64(startBlock)
}

var walkIndexfileFunc = walkIndexfile

func (p *Pipeline) Upload(baseIndex uint64, indexPath string) (err error) {
	dstoreOperationTimeout := 90 * time.Second
	hardOperationTimeout := 100 * time.Second // Protecting ourselves against dstore misbehaving

	destinationPath := fmt.Sprintf("shards-%d/%010d.bleve.tar.zst", p.shardSize, baseIndex)

	zlog.Info("upload: index", zap.Uint64("base", baseIndex), zap.String("destination_path", destinationPath))

	// TODO: refactor this so that writeObject function on the dstore should take a context
	p.indexesStore.SetOperationTimeout(dstoreOperationTimeout)

	pipeRead, pipeWrite := io.Pipe()
	writeDone := make(chan error)
	go func() {
		writeDone <- p.indexesStore.WriteObject(destinationPath, pipeRead) // to Google Storage
	}()

	tw := tar.NewWriter(pipeWrite)

	err = walkIndexfileFunc(indexPath+"/", tw)
	if err != nil {
		return fmt.Errorf("creating archive: %s", err)
	}

	zlog.Debug("closing tarWriter")
	if err = tw.Close(); err != nil {
		return fmt.Errorf(".tar.zst close: base %d: %s", baseIndex, err)
	}

	zlog.Debug("closing pipe")
	if err = pipeWrite.Close(); err != nil {
		return fmt.Errorf("write pipe close: base %d: %s", baseIndex, err)
	}

	select {
	case err = <-writeDone:
		if err != nil {
			return fmt.Errorf("writing to google storage: %s", err)
		}
		zlog.Info("archive upload done", zap.Uint64("base", baseIndex))
	case <-time.After(hardOperationTimeout):
		zlog.Error("upload hard timeout hit (dstore did not trigger timeout correctly)", zap.Uint64("base", baseIndex), zap.Duration("hard_operation_timeout", hardOperationTimeout))
		return fmt.Errorf("upload hard timeout hit")
	}

	return nil
}

// reads an index file on disk and copies it to an io.Writer
func walkIndexfile(indexPath string, tw *tar.Writer) error {
	return filepath.Walk(indexPath+"/", func(path string, info os.FileInfo, inErr error) error {
		if info.IsDir() {
			return nil
		}

		if inErr != nil {
			return fmt.Errorf("error walking %s: %s", path, inErr)
		}

		zlog.Debug("open path", zap.String("file", path))
		fl, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening %s: %s", path, err)
		}

		defer func() {
			err2 := fl.Close()
			if err == nil {
				err = err2
			}
		}()

		fileHeader := &tar.Header{
			Name: info.Name(),
			Mode: 0644,
			Size: info.Size(),
		}
		err = tw.WriteHeader(fileHeader)
		if err != nil {
			return fmt.Errorf("writing header %s: %s", path, err)
		}

		if _, err := io.Copy(tw, fl); err != nil {
			return fmt.Errorf("writing content %s: %s", path, err)
		}

		return nil
	})
}
