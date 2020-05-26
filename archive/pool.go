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
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/blevesearch/bleve/index/scorch"
	"github.com/dfuse-io/dmesh"
	dmeshClient "github.com/dfuse-io/dmesh/client"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/search"
	"github.com/dfuse-io/search/archive/roarcache"
	"go.uber.org/zap"
)

type IndexPool struct {
	ReadOnlyIndexesPaths []string // list of paths where to load on start
	IndexesPath          string   //local path where indices are stored on disk
	ShardSize            uint64

	ready bool

	SearchPeer  *dmesh.SearchPeer
	dmeshClient dmeshClient.Client

	indexesStore            dstore.Store
	LowestServeableBlockNum uint64

	// read indexes are indexes opened in read-only, that are already optimized
	//readPoolStartBlock uint64
	readPoolLock    sync.RWMutex // Level 2 lock
	ReadPool        []*search.ShardIndex
	PerQueryThreads int // Each end-user query will parallelize sub-queries on 15K+ indices

	emptyResultsCache roarcache.Cache
}

var numberOfPoolInitWorkers = 16 // During process bootstrap - AVOID too high value - there is contention
var numberOfAnalysisWorkers = 2  // Only used for indexing and merging (not for read-only)

func NewIndexPool(indexesPath string, readOnlyIndexesPaths []string, shardSize uint64, indexesStore dstore.Store, cache roarcache.Cache, dmeshClient dmeshClient.Client, searchPeer *dmesh.SearchPeer) (*IndexPool, error) {
	pool := &IndexPool{
		IndexesPath:          indexesPath,
		ReadOnlyIndexesPaths: readOnlyIndexesPaths,
		ShardSize:            shardSize,
		indexesStore:         indexesStore,
		emptyResultsCache:    cache,
		dmeshClient:          dmeshClient,
		SearchPeer:           searchPeer,
	}
	return pool, nil
}

func (p *IndexPool) IsReady() bool {
	return p.ready
}

// SetReady marks the process as ready, meaning it has crossed the "close
// to real-time" threshold.
func (p *IndexPool) SetReady() error {
	// TODO: implement a Ready = false when shutting down, like in the `live`.
	p.SearchPeer.Locked(func() {
		p.SearchPeer.Ready = true
	})
	err := p.dmeshClient.PublishNow(p.SearchPeer)
	if err != nil {
		return err
	}

	p.ready = true

	return nil
}

func (p *IndexPool) PollRemoteIndices(startBlockNum, stopBlockNum uint64) {
	startIndexingAt := startBlockNum
	lastIndexLoaded := p.LastReadOnlyIndexedBlock()
	headBlockNumber.SetUint64(lastIndexLoaded)
	if lastIndexLoaded > startIndexingAt {
		startIndexingAt = lastIndexLoaded + 1
	}
	if stopBlockNum != 0 && startIndexingAt >= stopBlockNum {
		zlog.Info("doing any index polling, we reached stopBlockNum already", zap.Uint64("stop_block_num", stopBlockNum))
		return
	}
	zlog.Info("polling indexes from remote storage", zap.Uint64("base_block_num", startIndexingAt))

	for {
		indexStartBlockNum := p.nextReadOnlyIndexBlock()
		// We may need to starting syncing a later index for a middle tier
		if indexStartBlockNum == 0 {
			indexStartBlockNum = startBlockNum
		}

		indexBaseFile := fmt.Sprintf("%010d", indexStartBlockNum)

		// we could parallelize this, but probably not useful, since they will
		// be produced as we go by indexers
		idx, err := p.retrieveIndexFile(indexStartBlockNum, indexBaseFile)
		if err != nil {
			if err.Error() != "index file is not available" {
				zlog.Info("cannot retrieve next index file, retrying in 5 seconds",
					zap.String("basefile", indexBaseFile),
					zap.Error(err))
			}
			time.Sleep(5 * time.Second)
			continue
		}

		if searchPeer := p.SearchPeer; searchPeer != nil {
			searchPeer.Locked(func() {
				searchPeer.IrrBlock = idx.EndBlock
				searchPeer.IrrBlockID = idx.EndBlockID
				searchPeer.HeadBlock = idx.EndBlock
				searchPeer.HeadBlockID = idx.EndBlockID
			})
			err = p.dmeshClient.PublishNow(searchPeer)
			if err != nil {
				zlog.Warn("unable to publisher search peer", zap.Error(err))
				continue
			}
		}

		headBlockNumber.SetUint64(idx.EndBlock)

		// PUBLISHER HERE
		zlog.Info("index file successfully retrieved",
			zap.String("basefile", indexBaseFile),
			zap.Uint64("start_block", idx.StartBlock))
	}
}

func (p *IndexPool) retrieveIndexFile(indexStartBlockNum uint64, indexBaseFile string) (*search.ShardIndex, error) {

	indexPath := fmt.Sprintf("shards-%d/%s.bleve.tar.zst", p.ShardSize, indexBaseFile)

	zlog.Debug("looking for index file",
		zap.String("index_path", indexPath),
		zap.String("base_file", indexBaseFile),
		zap.Uint64("start_block", indexStartBlockNum))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	found, err := p.indexesStore.FileExists(ctx, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed checking existence of index file: %s", err)
	}

	if !found {
		return nil, fmt.Errorf("index file is not available")
	}
	// TODO: can someone delete it right at this moment?

	err = p.downloadAndExtract(0, indexBaseFile)
	if err != nil {
		return nil, fmt.Errorf("error downloading and extracting index file: %s", err)
	}

	idx, err := p.openReadOnly(indexStartBlockNum)
	if err != nil {
		return nil, fmt.Errorf("error opening and reading next index file from disk: %s", err)
	}

	statsMap := idx.StatsMap()
	indexBytes := statsMap["CurOnDiskBytes"].(uint64)
	indexFiles := statsMap["CurOnDiskFiles"].(uint64)
	zlog.Debug("opened read-only index from disk",
		zap.Uint64("base", idx.StartBlock),
		zap.Uint64("bytes", indexBytes),
		zap.Uint64("files", indexFiles))

	// AppendReadIndexes does not lock read only pool
	p.AppendReadIndexes(idx)
	zlog.Debug("appended index file",
		zap.String("index_path", indexPath),
		zap.String("index_basefile", indexBaseFile),
		zap.Uint64("base", idx.StartBlock))

	return idx, nil
}

// NextReadOnlyIndexBlock returns the start block of the next index (999)
func (p *IndexPool) nextReadOnlyIndexBlock() uint64 {
	// should this be a read lock
	p.readPoolLock.Lock()
	defer p.readPoolLock.Unlock()

	if len(p.ReadPool) == 0 {
		return 0
	}
	lastIndexShard := p.ReadPool[len(p.ReadPool)-1]
	return (lastIndexShard.StartBlock + p.ShardSize)
}

func (p *IndexPool) SyncFromStorage(startBlock, stopBlock uint64, maxIndexes int, parallelDownloads int) error {
	totalDownloads := 0
	count := 0
	for {
		count++
		zlog.Info("launching sync pass", zap.Int("count", count))
		downloadCount, err := p.syncFromStoragePass(startBlock, stopBlock, maxIndexes, parallelDownloads)
		if err != nil {
			return err
		}

		totalDownloads += downloadCount

		if downloadCount == 0 {
			break
		}
	}

	zlog.Info("sync from storage done", zap.Int("downloaded_indexes", totalDownloads))

	return nil
}

func startBlockFromFileName(filename string) uint64 {
	startBlock, _ := strconv.ParseInt(filename, 10, 64)
	return uint64(startBlock)
}

func (p *IndexPool) syncFromStoragePass(startBlock, stopBlock uint64, maxIndexes int, parallelDownloads int) (numDownloads int, err error) {
	local, seenLocal, err := p.listAllReadOnlyIndexes()
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	remote, err := p.indexesStore.ListFiles(ctx, fmt.Sprintf("shards-%d/", p.ShardSize), ".tmp", maxIndexes+int(startBlock/p.ShardSize))
	if err != nil {
		return 0, err
	}

	zlog.Info("number of indices found", zap.Int("local_indexes", len(local)), zap.Int("remote_indexes", len(remote)))

	remotePathRE := regexp.MustCompile(`(\d{10})\.bleve\.tar\.zst`)
	seenRemote := make(map[string]string)

	count := 0
	for _, file := range remote {
		match := remotePathRE.FindStringSubmatch(file)
		if match == nil {
			zlog.Info("Skipping non-index file in remote storage", zap.String("file", file))
			continue
		}

		fileStartBlock := startBlockFromFileName(match[1])
		if fileStartBlock < startBlock {
			count++
			if count%1000 == 0 {
				zlog.Info("skipping index file before start block 1/1000",
					zap.String("file", file),
					zap.Int("skipped_file_count", count),
					zap.Uint64("start_block", startBlock),
				)
			}
			continue
		}
		if stopBlock != 0 && fileStartBlock >= stopBlock {
			zlog.Info("skipping index files >= stop block",
				zap.String("file", file),
				zap.Uint64("stop_block", stopBlock),
			)
			break
		}

		seenRemote[match[1]] = file
	}

	var toDownload []string
	for remoteFile := range seenRemote {
		if _, found := seenLocal[remoteFile]; found {
			continue
		}
		toDownload = append(toDownload, remoteFile)
	}

	sort.Strings(toDownload)

	// check for longest contiguous [local+remote] streak starting at startBlock

	downloadStopBlock := stopBlock
	for check := startBlock; stopBlock == 0 || check < stopBlock; check += p.ShardSize {
		indexBase := fmt.Sprintf("%010d", check)
		indexPath := fmt.Sprintf("shards-%d/%s.bleve.tar.zst", p.ShardSize, indexBase)
		if seenLocal[indexPath] {
			continue
		}
		if _, ok := seenRemote[indexBase]; ok {
			continue
		}
		downloadStopBlock = check
		break
	}

	zlog.Info("number of indices to download", zap.Int("count", len(toDownload)))

	eg := llerrgroup.New(parallelDownloads)
	for i, fl := range toDownload {
		if eg.Stop() {
			break
		}
		if downloadStopBlock != 0 && startBlockFromFileName(fl) >= downloadStopBlock {
			zlog.Info("Not downloading remote index because it would create an hole", zap.String("filename", fl))
			break
		}
		index := i
		filename := fl
		numDownloads += 1

		eg.Go(func() error {
			return p.downloadAndExtract(index, filename)
		})
	}

	// Download in parallel those files
	// Check what we have from disk (largest index)
	// If we don't have any new file for 5 seconds *AND* that all our local downloads
	// are done, then we continue on with any live process, feeding from block logs, etc..
	// In that case, we might re-index some schtuff, but that's life.
	return numDownloads, eg.Wait()
}

func (p *IndexPool) downloadAndExtract(index int, baseFile string) error {
	src := fmt.Sprintf("shards-%d/%s.bleve.tar.zst", p.ShardSize, baseFile)

	level := zap.DebugLevel
	if index%50 == 0 {
		level = zap.InfoLevel
	}

	zlog.Check(level, "downloading index").Write(zap.String("source", src))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	reader, err := p.indexesStore.OpenObject(ctx, src)
	if err != nil {
		return fmt.Errorf("opening object %q: %s", src, err)
	}
	defer reader.Close()

	tr := tar.NewReader(reader)

	dlPath := filepath.Join(p.IndexesPath, baseFile+"-dl.bleve")
	finalPath := filepath.Join(p.IndexesPath, baseFile+".bleve")

	_ = os.RemoveAll(dlPath)

	if err := os.MkdirAll(dlPath, 0755); err != nil {
		return err
	}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed reading next tar.zst index=%d, base=%s: %s", index, baseFile, err)
		}

		filename := filepath.Join(dlPath, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if _, err := os.Stat(filename); err != nil {
				if err := os.MkdirAll(filename, 0755); err != nil {
					return fmt.Errorf("Untar - cannot create directory, index=%d, base=%s: %s", index, baseFile, err)
				}
			}
		case tar.TypeReg:
			f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("Untar - cannot create file, index=%d, base=%s: %s", index, baseFile, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				return fmt.Errorf("Untar - cannot io.Copy failed, index=%d, base=%s: %s", index, baseFile, err)
			}
			err = f.Close()
		}
		if err != nil {
			return fmt.Errorf("Untar - uncaught, index=%d, base=%s: %s", index, baseFile, err)
		}
	}

	zlog.Check(level, "swapping index from download to read-only index path").Write(zap.String("src", dlPath), zap.String("dst", finalPath))
	return os.Rename(dlPath, finalPath)
}

func (p *IndexPool) CleanOnDiskIndexes(startBlock, stopBlock uint64) error {
	indexes, _, err := p.listAllReadOnlyIndexes()
	if err != nil {
		return err
	}

	zlog.Info("cleaning on disk indexes", zap.Int("nbr_index_on_disk", len(indexes)))

	for _, baseFile := range indexes {
		indexFileBaseBlockNum, err := strconv.ParseUint(baseFile, 10, 32)
		if err != nil {
			return err
		}

		if indexFileBaseBlockNum < startBlock || (stopBlock != 0 && indexFileBaseBlockNum >= stopBlock) {
			fullPath := filepath.Join(p.IndexesPath, baseFile+".bleve")

			err := os.RemoveAll(fullPath)
			zlog.Info("cleaning up on disk index that is before start block or >= stop block",
				zap.String("path", fullPath),
				zap.Uint64("start_block", startBlock),
				zap.Uint64("stop_block", stopBlock),
				zap.Error(err))

		}
	}
	return nil
}

func (p *IndexPool) ScanOnDiskIndexes(startBlock uint64) error {
	indexes, _, err := p.listAllReadOnlyIndexes()
	if err != nil {
		return err
	}

	// This is to ensure a sorted order loading of indexes
	indexesReady := make(chan chan *search.ShardIndex, numberOfPoolInitWorkers*2)
	indexesReadyDone := make(chan struct{})
	indexCount := 0

	go func() {
		for {
			idxCh := <-indexesReady
			if idxCh == nil {
				close(indexesReadyDone)
				return
			}

			idx, ok := <-idxCh
			if !ok {
				zlog.Error("idx channel did not receive an index, skipping index file, this should not happen")
				continue
			}

			indexCount++

			level := zap.DebugLevel
			if indexCount%50 == 0 {
				level = zap.InfoLevel
			}
			zlog.Check(level, "appending index").Write(zap.Uint64("base", idx.StartBlock))
			p.AppendReadIndexes(idx)
		}
	}()

	eg := llerrgroup.New(numberOfPoolInitWorkers)
	var previousIndex uint64

	for _, baseFile := range indexes {
		indexFileBaseBlockNum, err := strconv.ParseUint(baseFile, 10, 32)
		if err != nil {
			return err
		}

		if previousIndex != 0 && previousIndex+p.ShardSize != indexFileBaseBlockNum {
			zlog.Info("non-contiguous indexes on disk",
				zap.Int("number_of_indexes", len(indexes)),
				zap.Any("indexes", indexes),
				zap.Uint64("previous_index", previousIndex),
				zap.Uint64("current_index", indexFileBaseBlockNum),
			)
			time.Sleep(10 * time.Second)
			return fmt.Errorf("non-contiguous indexes on disk: %d followed by %d", previousIndex, indexFileBaseBlockNum)
		}
		previousIndex = indexFileBaseBlockNum

		// TODO: test for existence of `{match}/merging`, if so, relaunch merge process
		// so it can continue where it left off..

		if eg.Stop() {
			break
		}
		indexReady := make(chan *search.ShardIndex)
		indexesReady <- indexReady
		eg.Go(func() error {
			idx, err := p.openReadOnly(indexFileBaseBlockNum)
			if err != nil {
				zlog.Error("unable to open read only indexes",
					zap.Uint64("idx_start_block", indexFileBaseBlockNum),
					zap.Error(err),
				)
				close(indexReady)
				return err
			}

			statsMap := idx.StatsMap()
			indexBytes := statsMap["CurOnDiskBytes"].(uint64)
			indexFiles := statsMap["CurOnDiskFiles"].(uint64)
			zlog.Debug("opening initial read-only index from disk",
				zap.Uint64("base", idx.StartBlock),
				zap.Uint64("bytes", indexBytes),
				zap.Uint64("files", indexFiles),
			)

			// TODO: warm up the index

			indexReady <- idx

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		zlog.Error("error loading an index", zap.Error(err))
		return err
	}
	close(indexesReady)

	<-indexesReadyDone

	return nil
}

func (p *IndexPool) getReadOnlyIndexFilePath(baseBlockNum uint64) string {
	basePath := fmt.Sprintf("%010d.bleve", baseBlockNum)
	for _, path := range p.ReadOnlyIndexesPaths {
		fullPath := filepath.Join(path, basePath)
		if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
			return fullPath
		}
	}
	return filepath.Join(p.IndexesPath, basePath)
}

func (p *IndexPool) buildWritableIndexFilePath(baseBlockNum uint64, suffix string) string {
	if suffix != "" {
		suffix = "-" + suffix
	}

	return filepath.Join(p.IndexesPath, fmt.Sprintf("%010d%s.bleve", baseBlockNum, suffix))
}

func (p *IndexPool) openReadOnly(baseBlockNum uint64) (*search.ShardIndex, error) {
	path := p.getReadOnlyIndexFilePath(baseBlockNum)
	idxer, err := scorch.NewScorch("data", map[string]interface{}{
		"forceSegmentType": "zap",
		"forceSegmentVersion": 14,
		"read_only": true,
		"path":      path,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("creating scorch index: %s", err)
	}

	err = idxer.Open()
	if err != nil {
		return nil, fmt.Errorf("opening scorch index: %s", err)
	}

	// TODO: Warm up before adding?

	return search.NewShardIndexWithAnalysisQueue(baseBlockNum, p.ShardSize, idxer, p.buildWritableIndexFilePath, nil)
}

func (p *IndexPool) CloseIndexes() (err error) {
	for _, idx := range p.ReadPool {
		idx.Lock.Lock()
		defer idx.Lock.Unlock()

		if err = idx.Close(); err != nil {
			return err
		}
	}

	// TODO: Go through the ForkDB, and close all the `Object`
	// references.

	return nil
}

// LastReadOnlyIndexedBlock returns the block inclusively (999)
func (p *IndexPool) LastReadOnlyIndexedBlock() uint64 {
	if len(p.ReadPool) == 0 {
		return 0
	}
	idx := p.ReadPool[len(p.ReadPool)-1]
	return idx.EndBlock
}

func (p *IndexPool) LastReadOnlyIndexedBlockID() string {
	if len(p.ReadPool) == 0 {
		return ""
	}
	idx := p.ReadPool[len(p.ReadPool)-1]
	return idx.EndBlockID
}

func (p *IndexPool) AppendReadIndexes(idx ...*search.ShardIndex) {
	p.ReadPool = append(p.ReadPool, idx...)
}

func (p *IndexPool) GetLowestServeableBlockNum() uint64 {
	return p.LowestServeableBlockNum
}

func (p *IndexPool) SetLowestServeableBlockNum(startBlockNum uint64) error {
	// TODO: use a read lock here??
	p.readPoolLock.RLock()
	defer p.readPoolLock.RUnlock()

	if len(p.ReadPool) != 0 {
		idx := p.ReadPool[0]
		if idx.StartBlock != startBlockNum {
			return fmt.Errorf("first read-only index (start-end: %d-%d) mis-aligned with proposed start block %d", idx.StartBlock, idx.EndBlock, startBlockNum)
		}
	}

	p.LowestServeableBlockNum = startBlockNum

	return nil
}

func (p *IndexPool) LowestServeableBlockNumAbove(blockNum uint64) uint64 {
	p.readPoolLock.RLock()
	defer p.readPoolLock.RUnlock()
	for _, idx := range p.ReadPool {
		if idx.StartBlock > blockNum {
			return idx.StartBlock
		}
	}
	return 0
}

func (p *IndexPool) truncateBelow(blockNum uint64) {
	p.readPoolLock.Lock()
	defer p.readPoolLock.Unlock()

	for index, idx := range p.ReadPool {
		if idx.StartBlock >= blockNum {
			// index is above the block num
			newReadPool := []*search.ShardIndex{}
			for i := index; i < len(p.ReadPool); i++ {
				newReadPool = append(newReadPool, p.ReadPool[i])
			}
			p.ReadPool = newReadPool
			return
		}
		// index is below the blockNum should truncate it
		indexToRemove := idx
		go func() {
			zlog.Info("truncating index", zap.Uint64("idx_start_block", indexToRemove.StartBlock))

			indexToRemove.Lock.Lock()
			defer indexToRemove.Lock.Unlock()

			if err := indexToRemove.Close(); err != nil {
				zlog.Warn("error closing index", zap.Uint64("idx_start_block", indexToRemove.StartBlock), zap.Error(err))
				return
			}
			zlog.Info("index closed", zap.Uint64("idx_start_block", indexToRemove.StartBlock))

			p.deleteIndex(indexToRemove)
		}()
	}
}

func (p *IndexPool) deleteIndex(idx *search.ShardIndex) {
	baseFile := fmt.Sprintf("%010d", idx.StartBlock)
	fullPath := filepath.Join(p.IndexesPath, baseFile+".bleve")
	err := os.RemoveAll(fullPath)
	zlog.Info("removed on disk index",
		zap.String("path", fullPath),
		zap.Error(err))
}
