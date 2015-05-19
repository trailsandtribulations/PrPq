# PrPq - Promise-Based Node / Postgres Module

A simple Promise-base node.js module for Postgres, based on Brain Carlson\'s
[node-libpq](https://github.com/brianc/node-libpq).

Like [node-postgres](https://github.com/brianc/node-postgres), PrPq does its own
connection pooling, howbeit not quite as resilient as pg\'s, and not quite as fast.
A straight forward approach that offers a way out of callback hell.

````javascript
let pq = new (require('./PrPq'))();
pq.conn( connStr, 'set search_path=foo' )
.then( () => pq.query( 'select * from bar where usr_id=$1', [usrId] ) )
.then( () => {
	if( pq.rowCount() ) == 0 ) return Promise.reject( 'bar record not found' );
	usr = pq.row(0);
	return pq.query( 'select * from barbar where usr_id=$1', [usrId] ) )
})
.then( () => {
	usrbars = pq.rows();
	pq.end();
}
.catch( err => {
	console.log( 'ERROR: '+err );
	pq.end();
});
````

>PrPq is not the same as [pg-promise](https://github.com/vitaly-t/pg-promise) in substantive
>ways. PrPq is more of an approach for those who are already fluent in postgres, whereas
>pg-promise if for those who already use pg. Unlike pg-promise, PrPq is based on lib-pg,
>not pg; does not use ps\'s connection pool; does not share connections
>between queries; does not support pg based prepared statements; does not support
>some automatic data conversion, eg, `timestamp` to `new Date()`.

## Theory of Operation

The general work flow is
- `let rcds = null;`
- `let pq = new PrPq()` - create a new PrPq instance
- `pq.conn(...)` - grab a connection for this instance\s session
- `then( () => ...; return pq.query(... ) )` - query the database
- `then( () => rcds = pq.rows; return Promise.resolve(pq) )` - do something with results, return promise
- `then( () => return pq.end() )` - close 
- `then( () => ... )` - sail on

results, ..., then `end()`. A resolved Promise always returns the PrPq instance.

Connection and query methods return a Promise. Other return data out of the results.

_Before a query is called, the previous query\'s results are cleared._


## Promise Returning Functions

### Connection Functions

`pq.conn( connectString, [ initializationQuery ] ) => Promise`

- the `connectString` uses `libpq` standards.
- sessions are pooled according to `connectString`
- the optional `initializationQuery` is performed once when the session is created
  on the server for that `connectString`
	- executed the first, _not_ each time, the connection is used
	- use different `connectString` for different `initializationQuery`s

`pq.end() => Promise`
	- if in transaction, rolls back
	- clears results
	- puts connection back into the pool

`pq.finish() => Promise`
	- `pq.end().then()`
	- closes connection to database
	- clears connection from pool

## Query Functions

`pq.query( queryString, [ args ] ) => Promise`

- `args` is optional - if existent, simply calls `queryParams()`

`pq.queryParams( queryString, [ args ] ) => Promise`

`pq.prepare( name, queryString, [ types ], ignoreExists ) => Promise`

- create a Prepared Statement
- if `ignoreExists`, ignores error if a Prepared Statement of that name already exists
- `types` optional, default null
- `ignoreExists` optional, default false

`pq.execute( name, [ args ] ) => Promise`
`pq.queryPrepared( name, [ args ] ) => Promise`

- execute a prepared statement

`pq.deallocate( [ name ], ignoreNotExists ) => Promise`

- deallocate the prepared statement
- `name` optional, default "ALL"
- `ignoreNotExists` optional, default false
- if no `name`, deallocates ALL Prepared Statements in the session
- if `ignoreNotExists`, ignores error if a Prepared Statement of that name does not exist


### Transaction Functions

User directly queries `begin, commit, rollback` at her own risk. `savepoint` is not supported.

`pq.begin() => Promise`

	- begin a transaction

`pq.commit( ignoreNoTransaction ) => Promise`

	- commit a transaction
	- `ignoreNoTransaction` is optional, default false
	- if `ignoreNoTransaction` then "No Transaction in Progress" error ignored

`pq.rollback( ignoreNoTransaction ) => Promise`

	- rolls back the transaction
	- `ignoreNoTransaction` is optional, default false
	- if `ignoreNoTransaction` then "No Transaction in Progress" error ignored

## Non-Promise Returning Functions

### Configuration

- `PrPq.setMAXCONNECTIONS(ct)` 
	- sets the maximum number of pool connections
	- will never be set lower than the current number of pooled connections
- `PrPq.MAXCONNETIONS` - get max total pool size over all subpools

### State Functions

- `pq.inTrans` - get whether in transaction

### Result Functions

- `pq.rowCount` - get number of rows retreived by a query
- `pq.colCount` - get number of cols in one row
- `pq.affectedRows` - get number of rows affected by DML
- `pq.rows` - get array of all rows retreived
- `pq.row([r])` - `r`th row. 0 if undefined
- `pq.col([r],[c])` - column value by number or name from `r`th row; 0 if undefined

### Use `libpq` Unfiltered

- `pq._conn.libpq` is the `libpq` instance within the `PrPq` instance
	- if `pq` is not connected, `pq._conn` is null
	- see [node-libpq](https://github.com/brianc/node-libpq) for methods
	- drive responsibly

