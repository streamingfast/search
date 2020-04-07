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
  echo "./exec-user-query.sh 'account:eosio.token receiver:eosio.token action:transfer (data.from:eoscanadacom OR data.to:eoscanadacom)' [LIMIT] [ASC/DESC] [START_BLOCK] [BLOCK_COUNT] [NETWORK]";
  exit 1;
fi

LIMIT="$2"
if [ -z "$LIMIT" ]; then
  LIMIT="100"
fi

ORDER_BY="$3"
if [ -z "$ORDER_BY" ]; then
  ORDER_BY="desc"; # ASC
elif [ "$ORDER_BY" == "ASC" ]; then
  ORDER_BY="asc";
else
  ORDER_BY="desc";
fi

START_BLOCK=0
if [ ! -z "$4" ]; then
  start_block="$4"
fi

BLOCK_COUNT="100000000"
if [ ! -z "$5" ]; then
  BLOCK_COUNT="$5"
fi

NETWORK="mainnet"
if [ ! -z "$6" ]; then
  NETWORK="$6"
fi


curl -s --get \
     -H "Authorization: Bearer $EOSWS_API_KEY" \
     --data-urlencode "start_block=$START_BLOCK" \
     --data-urlencode "block_count=$BLOCK_COUNT" \
     --data-urlencode "limit=$LIMIT" \
     --data-urlencode "sort=$ORDER_BY" \
     --data-urlencode "q=$QUERY" \
     --data-urlencode "with_reversible=true" \
     "https://$NETWORK.eos.dfuse.io/v0/search/transactions"
