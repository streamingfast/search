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

package archive

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

func (p *IndexPool) Upload(baseIndex uint64, indexPath string) (err error) {
	destinationPath := fmt.Sprintf("shards-%d/%010d.bleve.tar.zst", p.ShardSize, baseIndex)

	pipeRead, pipeWrite := io.Pipe()
	writeDone := make(chan error)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		writeDone <- p.indexesStore.WriteObject(ctx, destinationPath, pipeRead) // to Google Storage
	}()

	tw := tar.NewWriter(pipeWrite)

	err = filepath.Walk(indexPath+"/", func(path string, info os.FileInfo, inErr error) (err error) {
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

	if err = <-writeDone; err != nil {
		return fmt.Errorf("writing to google storage: %s", err)
	}

	zlog.Info("archive upload done", zap.Uint64("base", baseIndex))
	return nil
}
