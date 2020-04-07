#!/bin/bash
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

producers=(
'eoslaomaocom'
'eoshuobipool'
'bitfinexeos1'
'eoscannonchn'
'eosflytomars'
'atticlabeosb'
'eosnewyorkio'
'eosliquideos'
'eos42freedom'
'eosiosg11111'
'eosbixinboot'
'eosbeijingbp'
'eosriobrazil'
'eosauthority'
'eosswedenorg'
'eosdacserver'
'cochainworld'
'eoscanadacom'
'eosfishrocks'
'eospaceioeos'
'eosgenblockp'
'eosnationftw'
'eostitanprod'
'cypherglasss'
'argentinaeos'
'eoscafeblock'
'eospacificbp'
'eosasia11111'
'eosyskoreabp'
'eosiomeetone'
'cryptolions1'
'eostribeprod'
'eosstorebest'
'eoshenzhenio'
'eosvolgabpru'
'eosdotwikibp'
'eosantpoolbp'
'eosamsterdam'
'eosisgravity'
'eoscleanerbp'
'alohaeosprod'
'games.eos (k'
'eoswingdotio'
'eosdublinwow'
'eosfengwocom'
'aus1genereos'
'eosphereiobp'
'eoszhizhutop'
'eoseouldotio'
'eoscybexiobp'
'blocksmithio'
'eoslambdacom'
'blockmatrix1'
'eossv12eossv'
'eosmatrixeos'
'eosuestciobp'
'acroeos12345'
'eosplayworld'
'eoschaintech'
'eosmotioneos'
'eosvenezuela'
'geosoneforbp'
'eosnairobike'
'btccpooleos1'
'eosiodetroit'
'eosunion1111'
'eoscandyone1'
'eosukblocpro'
'eosnodeonebp'
'eosflareiobp'
'eosonoeosono'
'eosmetaliobp'
'eoswinwinwin'
'costaricaeos'
'auroraeoscom'
'blockgenicbp'
'eosrapidprod'
'eosmedinodes'
'eosvibesbloc'
'eoswenzhoubp'
'eosafricaone'
'eosbarcelona'
'bitspacenode'
'eosecoeoseco'
'dutcheosxxxx'
'eosmesodotio'
'1eostheworld'
'eosvancouver'
'eosmiamidade'
'dposclubprod'
'eosgermanybp'
'eosthushuimu'
'eosvietnambp'
'eosjapanclub'
'eossocalprod'
'eoswancloud1'
'eosarabianet'
'bpacbpacbpac'
'eosadddddddd'
)


producers_len=${#producers[@]}

for i in $(seq 0 1000); do
	producer_idx=$(($RANDOM % $producers_len))
	PRODUCER=${producers[$producer_idx]}
	
  QUERY="account:eosio.token receiver:eosio.token action:transfer (data.from:$PRODUCER OR data.to:$PRODUCER)"
	PAYLOAD="{\"limit\":10000,\"sort_desc\":false,\"query\":\"$QUERY\"}"
	
	echo "Getting transfers to $PRODUCER"
  time curl "http://$HOST/v0/search/transactions" --data "$PAYLOAD"
done
