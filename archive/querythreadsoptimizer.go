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
	"sync"

	"github.com/abourget/llerrgroup"
	"go.uber.org/zap"
)

func NewQueryThreadsOptimizer(maxThreads int, sortDesc bool, lowBlockNum uint64, llerrgroup *llerrgroup.Group) *queryThreadsOptimizer {
	qto := &queryThreadsOptimizer{
		maxThreads:   maxThreads,
		llerrgroup:   llerrgroup,
		sortDesc:     sortDesc,
		lastShardNum: -1,
	}

	if !sortDesc && lowBlockNum < 10000 { //arbitrary optimization..... this should depend on chain.. but better than nothing, will match block<3
		qto.setThreads(qto.maxThreads)
	} else {
		qto.setThreads(2)
	}
	return qto
}

type queryThreadsOptimizer struct {
	limit            int
	currentThreads   int
	maxThreads       int
	lastShardResults int
	lastShardNum     int
	passedFirstShard bool
	sortDesc         bool
	llerrgroup       *llerrgroup.Group
	lock             sync.Mutex
}

func (qto *queryThreadsOptimizer) ReportShard(shardMin, results int) {
	qto.lock.Lock()
	defer qto.lock.Unlock()
	if (qto.sortDesc && shardMin < qto.lastShardNum) || (!qto.sortDesc && shardMin > qto.lastShardNum) {
		if qto.lastShardNum != -1 {
			qto.passedFirstShard = true
		}
		qto.lastShardNum = shardMin
		qto.lastShardResults = results
	}
}

func (qto *queryThreadsOptimizer) Optimize() {
	qto.lock.Lock()
	defer qto.lock.Unlock()
	if qto.lastShardNum == -1 {
		return
	}

	// TODO: the limit isn't passed to backends now, so what to do with this?
	// if qto.lastShardResults == qto.limit {
	// 	if qto.passedFirstShard { // don't want this if >limit on first shard because of cursor
	// 		qto.setThreads(1) // dangerous to use 0 here, at least let it finish
	// 		return
	// 	}
	// }
	if qto.lastShardResults > 1000 {
		qto.setThreads(qto.maxThreads / 6)
		return
	}
	if qto.lastShardResults > 500 {
		qto.setThreads(qto.maxThreads / 4)
		return
	}
	if qto.lastShardResults > 100 {
		qto.setThreads(qto.maxThreads / 3)
		return
	}
	if qto.lastShardResults > 10 {
		qto.setThreads(qto.maxThreads / 2)
		return
	}
	qto.setThreads(qto.maxThreads)
}

func (qto *queryThreadsOptimizer) setThreads(t int) {
	if t != qto.currentThreads {
		qto.currentThreads = t
		qto.llerrgroup.SetSize(t)
		zlog.Debug("setting current threads for query", zap.Int("threads", t))
	}
}
