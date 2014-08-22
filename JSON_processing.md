#`imem_json:match/2` design

This file describes how `imem_json:match/2` module works together with JPParse

JSON Object:

```javascript
{ "some": "json" }
```

```json
{"a1": 123,
"a2": "abcd",
"a3": [1,2,3],
"a4": {"b1":456,b2:789},
"a5": {"b1":"string"},
"a6": [{"b1":1}, {"b1":2}, {"b1":3}],
"a7": []}
```

JPPath | JSON | Proplist
--- | --- | ---
`"root"` |  `{a1:123,a2:"abcd",a3:[1,2,3],a4:{b1:456,b2:789},a5:{b1:"string"},a6:[{b1:1},{b1:2},{b1:3}],a7:[]}` | ``
