# agg-pivot
create pivot table and aggregate it.

after
```
{
    "rows": [
        [ {"value": "1990","name": "year"} ],
        [ {"value": "1991","name": "year"} ],
        [ {"value": "1992","name": "year"} ]
    ],
    "cols": [
        [ {"value": "tom","name": "name"} ],
        [ {"value": "alice","name": "name"} ],
        [ { "value": "bob","name": "name"} ]
    ],
    "sums": [
        "sales",
        "budgets"
    ],
    "table": {
        "1990&tom&sales": 130,
        "1990&tom&budgets": 241,
        "1990&alice&sales": 460,
        "1990&alice&budgets": 245,
        "1990&bob&sales": 0,
        "1990&bob&budgets": 0,
        "1991&tom&sales": 678,
        "1991&tom&budgets": 124,
        "1991&alice&sales": 0,
        "1991&alice&budgets": 0,
        "1991&bob&sales": 0,
        "1991&bob&budgets": 0,
        "1992&tom&sales": 0,
        "1992&tom&budgets": 0,
        "1992&alice&sales": 0,
        "1992&alice&budgets": 0,
        "1992&bob&sales": 700,
        "1992&bob&budgets": 251
    }
}
```

before
```
[
    'year', 'month', 'name', 'sales', 'budgets',
    '1990', '0', 'tom', '123', '120',
    '1990', '1', 'tom', '7', '121',
    '1990', '1', 'alice', '456', '122',
    '1990', '2', 'alice', '4', '123',
    '1991', '2', 'tom', '678', '124',
    '1992', '3', 'bob', '664', '125',
    '1992', '4', 'bob', '36', '126',
]
```

## how to use

```

// table of data you want to aggregate
const buf = [
    'year', 'month', 'name', 'sales', 'budgets',
    '1990', '0', 'tom', '123', '120',
    '1990', '1', 'tom', '7', '121',
    '1990', '1', 'alice', '456', '122',
    '1990', '2', 'alice', '4', '123',
    '1991', '2', 'tom', '678', '124',
    '1992', '3', 'bob', '664', '125',
    '1992', '4', 'bob', '36', '126',
];

// create table and preprocess to aggregate with specifing count of header
// for example, the 'buf' has 5 headers 'year', 'month', 'name', 'sales', 'budgets'
const table = frombuf(buf, 5);

// you can see table info 
console.log(JSON.stringify(table));

// configure pivot table configration and aggregate its
const a = aggregate(table, {
    rows: ['year'],
    cols: ['name'],
    sums: ['sales', 'budgets'],
});

// goal
console.log(JSON.stringify(a));
```