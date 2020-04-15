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
	"encoding/hex"
	"strconv"
)

func MapFirstNChunks(input []byte, nChunks int, chunkSize int) map[string]string {
	mapData := make(map[string]string)
	DoForFirstNChunks(input, nChunks, chunkSize, func(idx int, chunk []byte) {
		mapData[strconv.Itoa(idx)] = trimZeroPrefix(hex.EncodeToString(chunk))
	})

	return mapData
}

func DoForFirstNChunks(input []byte, nchunks int, chunkSize int, function func(idxRef int, chunkRef []byte)) {
	chunks := splitInChunks(input, chunkSize)
	for idx, chunk := range chunks {
		if idx <= nchunks || nchunks == -1 {
			function(idx, chunk)
		} else {
			return
		}
	}
}

func splitInChunks(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}
