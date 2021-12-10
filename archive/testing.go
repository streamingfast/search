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
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	pb "github.com/streamingfast/pbgo/sf/search/v1"
	"github.com/streamingfast/search"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestNewClient(t *testing.T, searchEngine *ArchiveBackend) (pb.BackendClient, func()) {
	t.Helper()

	matchCollector := search.GetMatchCollector
	if matchCollector == nil {
		panic(fmt.Errorf("no match collector set, should not happen, you should define a collector"))
	}

	searchEngine.matchCollector = matchCollector
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterBackendServer(s, searchEngine)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Error("failed to dial bufnet", err)
		t.Fail()
	}

	client := pb.NewBackendClient(conn)
	return client, func() {
		// TODO: make sure we unlisten when we close this, or stop the server
		conn.Close()
		s.Stop()
	}
}
