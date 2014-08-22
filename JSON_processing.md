#`imem_json:match/2` design

This file describes how `imem_json:match/2` module works together with JPParse

JSON Object:

```javascript
{a1: 123,
 a2: "abcd",
 a3: [1,2,3],
 a4: {b1: 456, b2: 789},
 a5: {b1: "string"},
 a6: [{b1: 1}, {b1: 2}, {b1: 3}],
 a7: []}
```

JPPath | JSON | Proplist
--- | --- | ---
`"root"` |  `{a1:123,`<br>`a2:"abcd",`<br>`a3:[1,2,3],`<br>`a4:{b1:456,b2:789},`<br>`a5:{b1:"string"},`<br>`a6:[{b1:1},{b1:2},{b1:3}],`<br>`a7:[]}` | `[{<<"a1">>,123},`<br>` {<<"a2">>,<<"abcd">>},`<br>` {<<"a3">>,[1,2,3]},`<br>` {<<"a4">>,[{<<"b1">>,456},{<<"b2">>,789}]},`<br>` {<<"a5">>,[{<<"b1">>,<<"string">>}]},`<br>` {<<"a6">>,[[{<<"b1">>,1}],[{<<"b1">>,2}],[{<<"b1">>,3}]]},`<br>` {<<"a7">>,[]}]`
`"root1"` | `nomatch` | `nomatch`
`"root{}"` | `{}` | `[{}]`
`"root1{}"` | `nomatch` | `nomatch`
`"root{a0}"` | `nomatch` | `nomatch`
`"root{a7}"` | `{a7: []}` | `[{<<"a7">>,[]}]`
`"root{a4}"` | `{a4: {b1:456, b2:789}}` | `[{<<"a4">>,[{<<"b1">>,456},{<<"b2">>,789}]}]`
`"root:a1"` | `123` | `123`
`"root:a4"` | `{b1:456, b2:789}` | `[{<<"b1">>,456},{<<"b2">>,789}]`
`"root:a7"` | `[]` | `[]`
`"root:a4:b1"` | `456` | `456`
`"root{a4}:a4"`<br>(`"root:a4"`) | `{b1:456, b2:789}` | `[{<<"b1">>,456},{<<"b2">>,789}]`
`"root{a4}:b1"` | `nomatch` | `nomatch`
`"root{a4}:a4:b1"` | `456` | `456`
`"root{a4,a5}::b1"` | `[{"#path": ["root","a4","b1"],`<br>&nbsp;&nbsp;` "#value": 456}, `<br>` {"#path": ["root","a5","b1"],`<br>&nbsp;&nbsp;` "#value":"string"}]` | `[{<<"[\"root\",\"a5\",\"b1\"]">>,"string"},`<br>` {<<"[\"root\",\"a5\",\"b1\"]">>,<<"string">>}]`
