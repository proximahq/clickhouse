# `@prxm/clickhouse` : A dead simple Clickhouse client

## Installation
`npm install @prxm/clickhouse`

## Usage
```javascript
const clickhouse = require('@prxm/clickhouse');
const config = {
  host: 'localhost',
  protocol: 'http',
  user: 'default',
  password: 'password',
};
const client = clickhouse(config);

(async () => {
  await client.open();
  const res = await client.selectJson('SELECT number FROM system.numbers LIMIT 10;');
  console.log(res);
  await client.close();
})();
```

## API
The clickhouse client exposes several internal methods for usage

### `client.query(query, ?[params], ?queryId): Promise`

Send an async query to the HTTP interface.

##### `query: string`
SQL query statement.

```javascript
const res = await client.query(`SELECT * FROM foo WHERE a=? AND b=?`, ['hello', 'world']);
console.log(res.data, res.meta);
```

##### `params: sqlstring params`
Used for passing params to the query as
```javascript
client.query(`SELECT * FROM foo WHERE a=? AND b=?`, ['hello', 'world']);
```

##### `queryId: string`
Used as a unique `queryId` for the query.
```javascript
client.query(`SELECT * FROM foo;`, [], 'xxx');
```

### `client.selectJson(query, [params], ?queryId): Promise<JSON>`
Sends an async JSON query to the HTTP interface.

##### `query: string`
SQL query statement.

##### `params: sqlstring params`
Used for passing params to the query as
```javascript
client.selectJson(`SELECT * FROM foo WHERE a=? AND b=?`, ['hello', 'world']);
```

##### `queryId: string`
Used as a unique `queryId` for the query.
```javascript
client.selectJson(`SELECT * FROM foo;`, [], 'xxx');
```

### `client.insertBatch({table, items}, ?queryId): Promise`
Batch instert for tables.

##### `{table: string}`
The table's name.

##### `{items: [{}]}`
The items to insert, keys are used as the corresponding column names.

```javascript
const items = [
  {a: 1, b: 'foo', c: 3},
  {a: 1, b: 'baz', c: 3},
];
client.insertBatch({ table: 'batch',items});
```


##### `queryId: string`
Used as a unique `queryId` for the query.
```javascript
client.insertBatch({ table: 'batch',items}, 'xxx')
```

