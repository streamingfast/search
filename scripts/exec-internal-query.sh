# Copyright 2019 dfuse Platform Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

QUERY="$1"
if [ -z "$QUERY" ]; then
  echo "./exec-query.sh \"account:eosio.token action:transfer data.from:eoscanadacom\" [LIMIT] [DESC] [START_BLOCK] [HOST]";
  exit 1;
fi

LIMIT="$2"
if [ -z "$LIMIT" ]; then
  LIMIT="10000"
fi

ORDER_BY="$3"
if [ -z "$ORDER_BY" ]; then
  ORDER_BY="false"; # ASC
elif [ "$ORDER_BY" == "ASC" ]; then
  ORDER_BY="false";
else
  ORDER_BY="true";
fi

START_BLOCK=0
if [ ! -z "$4" ]; then
  START_BLOCK="$4"
fi

HOST="search.dfuse.io"
if [ ! -z "$5" ]; then
  HOST="$5"
fi

curl http://$HOST/v0/search/transactions --data "{\"limit\":$LIMIT,\"sort_desc\":$ORDER_BY,\"start_block\":$START_BLOCK,\"query\":\"${QUERY}\",\"block_count\":100000000,\"with_reversible\":true}"
