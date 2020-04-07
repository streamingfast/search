Query language
--------------

Examples:

```
receiver:hello account:eoscanadacom action:transfer
avec un espace:
receiver: hello account:eoscanadacom action:transfer
receiver:"hello world" account: eoscanadacom action: transfer
data.from:eoscanadacom action:transfer account:eosio.token
(data.from:eoscanadacom OR data.to:eoscanadacom OR data.quantity:"12323.2323 EOS") action:transfer account:eosio.token
(data.from:eoscanadacom OR data.to:eoscanadacom) data.quantity:"12323.2323 EOS" action:transfer account:eosio.token
(data.from:eoscanadacom data.to:eoscanadacom) OR (data.to:eoscanadacom data.from:mamabob)
db.op:update db.scope:eoscanadacom db.table:accounts db.account:eosio.token
ram.account:eoscanadacom
```

The current basic language supports a first level of ands, and a
single second level of parenthesized OR statements.

We keep it deliberately simple, so it can easily be implemented by
some Go code, and translated to the bleve engine to fast query.
