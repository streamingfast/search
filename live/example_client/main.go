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

package main

//var addr = "localhost:9000"

/*
  StartBlock:     0,
  Query:          "status:executed",
  BlockCount:     1000,
  SortDescending: true,
  Limit:          1,
  WithReversible: true,
*/
//query:"account:eosio.token receiver:eosio.token action:transfer data.from:newdexpocket data.to:marketmaker1" lowBlockNum:1 highBlockNum:53158931 descending:true cursor:"1:53152609::cb4ec76cf617" limit:100
//func main() {
//	req := &search.RouRequest{
//		LowBlockNum:  8350010,
//		HighBlockNum: 8350020,
//		//		UseLegacyBoundaries: true,
//		//		StartBlock:          0,
//		//		BlockCount:          math.MaxUint32,
//		//		LiveMarkerInterval: 10,
//		Descending:     true,
//		WithReversible: false,
//		//	Query:          "method:a9059cbb",
//		Query: "method:\"transfer(address,uint256)\"",
//
//		//		Cursor: "1:8350017::29739e2f239f",
//		Limit: 200,
//	}
//	conn, err := grpc.Dial(addr, grpc.WithInsecure())
//	if err != nil && err.Error() != "EOF" {
//		fmt.Println(err)
//		os.Exit(1)
//	}
//
//	ctx := context.Background()
//
//	fmt.Println("Trying connection to router client on", addr)
//
//	cli := pb.NewBackendClient(conn)
//
//	//cli := pb.NewRouterClient(conn)
//
//	scli, err := cli.StreamMatches(ctx, req)
//	if err != nil {
//		fmt.Println("got error now", err)
//		os.Exit(1)
//	}
//
//	for {
//		match, err := scli.Recv()
//		if err != nil {
//			fmt.Println(err)
//			fmt.Println(scli.Trailer())
//			break
//		}
//
//		fmt.Println(match.TrxIdPrefix, match.Cursor) //v.GetActionIndexes(), v.GetCursor(), scli.Trailer())
//
//		specificMatch := match.Specific
//
//		switch v := specificMatch.(type) {
//		case *pb.SearchMatch_Eos:
//			if v.Eos.Block != nil {
//				fmt.Println("some trace with ID", v.Eos.Block.BlockID)
//			}
//
//		case *pb.SearchMatch_Eth:
//			spew.Dump(v.Eth.Block)
//			// Skip for now
//
//		default:
//			panic("logic error, none of the known match payload type is defined")
//		}
//	}
//
//	os.Exit(0)
//
//	fmt.Println("Trying connection to archive client on", addr)
//	cli2 := pb.NewBackendClient(conn)
//	for {
//
//		counter := 0
//		rcli, err := cli2.StreamMatches(ctx, req)
//		if err != nil {
//			fmt.Println(err)
//			os.Exit(1)
//		}
//
//		for {
//			match, err := rcli.Recv()
//			if err != nil {
//				fmt.Println(err)
//				if counter != int(req.Limit) {
//					return
//				}
//				break
//			}
//
//			specificMatch := match.Specific
//
//			switch v := specificMatch.(type) {
//			case *pb.SearchMatch_Eos:
//				fmt.Println(match.GetTrxIdPrefix(), v.Eos.GetActionIndexes(), match.GetCursor())
//
//			case *pb.SearchMatch_Eth:
//				fmt.Println(match.GetTrxIdPrefix(), match.GetCursor())
//
//			default:
//				panic("logic error, none of the known match payload type is defined")
//			}
//
//			req.Cursor = match.Cursor
//			highBlockNumStr := strings.Split(match.Cursor, ":")[1]
//			highBlockNum, _ := strconv.ParseInt(highBlockNumStr, 10, 64)
//			req.HighBlockNum = highBlockNum
//
//			counter++
//		}
//	}
//}
