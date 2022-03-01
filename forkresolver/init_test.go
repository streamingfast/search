package forkresolver

import "github.com/streamingfast/bstream"

func init() {
	bstream.GetBlockReaderFactory = bstream.TestBlockReaderFactory
}
