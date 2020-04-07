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

HOST="search-staging.dfuse.io"
if [ ! -z "$1" ]; then
  HOST="$1"
fi

QUERY="account:eosio.msig action:exec data.proposer:eoscanadacom"
PAYLOAD="{\"limit\":1000, \"sort_desc\":false, \"query\":\"$QUERY\"}"
curl http://$HOST/v0/search/transactions --data "$PAYLOAD"