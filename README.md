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
  const res = await client.selectJson('SELECT number FROM system.numbers LIMIT 10;');
  console.log(res);
})();
```

## API
The clickhouse client exposes several internal methods for usage

### `client.query(query, [params], [extras]): Promise`

Send an async query to the HTTP interface.

##### `query: string`
SQL query statement.

```javascript
const res = await client.selectJson(`SELECT * FROM foo WHERE a=? AND b=?`, ['hello', 'world']);
console.log(res.data, res.meta);
```

##### `params: sqlstring params`
Used for passing params to the query as
```javascript
client.selectJson(`SELECT * FROM foo WHERE a=? AND b=?`, ['hello', 'world']);
```

##### `extras: {}`
Extra options to pass along the query, useful when targeting different databases.
```javascript
client.selectJson(`SELECT * FROM foo;`, [], {db: 'test'});
```

### `client.selectJson(query, [params], [extras]): Promise<JSON>`
Sends an async JSON query to the HTTP interface.

##### `query: string`
SQL query statement.

##### `params: sqlstring params`
Used for passing params to the query as
```javascript
client.selectJson(`SELECT * FROM foo WHERE a=? AND b=?`, ['hello', 'world']);
```

##### `extras: {}`
Extra options to pass along the query, useful when targeting different databases.
```javascript
client.selectJson(`SELECT * FROM foo;`, [], {db: 'test'});
```

### `client.insertBatch({table, items}, [extras]): Promise`
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


##### `extras: {}`
Extra options to pass along the query, useful when targeting different databases.
```javascript
client.insertBatch({table: 'foo', items: [{a:1}]}, {db: 'test'});
```

### _beta_
### `client.queryStream({query, params, opaque, factory}): Promise<StreamData>`
Sends an async query to the HTTP interface and returns a stream.
Based on the `undici` library this API can be used to stream data from ClickHouse, back to the server.
