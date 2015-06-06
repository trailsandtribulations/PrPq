// simple-promise-libpq
//

/* call conn(connStr,initSQL)
 * - returns a promise of new PrPq instance
 * - connected to db
 * queries return a promise which resolve to the same PrPq instance
 * - .then() - process query
 * - note: each query clears results from previous query before excuting
 * query results: rows, row(), col(), etc
 * - return array, object, scalar, etc
 * end() - cleans up and returns connection to pool
 * finish() - cleans up and drops connection
 *
 * transactions:
 * - begin() - begins trans, returns Promise
 * - commit() - commits trans, returns Promise
 * - rollback() - rolls back trans, returns Promise
 * - inTrans() - get whether instance inside transaction
 *
 * types (as read from query):
 * - oid, integer, float and bool are converted to int, int, float, boolean respectively
 * - all others are left as string
 * - null are always represented as null
 */

/* security
 *
 * level 2 pg security
 * - defines security between node and postgres
 *   = enforced by postgres through grants on "www-data", the node user
 *   = if node hacked or insecure, limit exposure to the database
 * - no DML, only SELECT, permitted on tables or views
 *   = of course node, not the client, should format each clauses of a SELECT before passing to pg
 *   = parameterized SELECT should be used to hide raw client data
 * - all DML goes through funcs owned by pgadmin
 * - cannot create or alter tables, triggers, views, functions, users
 *
 * level 1 pg security, like level 2 execpt:
 * - SELECT must go through functions owned by pgadmin
 *
 * node's abilities constrained
 * - cannot create, and no need to track, prepared statements
 * - pooling (recomended) by 3rd party software, eg, pgbouncer
 * 
 */

'use strict';

// class PrPq - empty shell until connected
var PrPq = () => {
  this._inTrans = false;
  this._conn = null;
}
module.exports = PrPq;

/* connect to db
 * - connStr as documented by libpq
 * - initSQL is SQL ; separated SQL statements that will be run upon connection
 *   = eg, "set search_path = ts_app; set time zone 'UTC'"
 *   = eg, "select ts_app.initPrepare()"
*/
PrPq.prototype.conn = ( connStr, initSQL ) => {
  let pq = this;
  //-console.log( 'conn: '+connStr );
  return new Promise( (resolve,reject) => {
    // have connection in pool - use it
    if( PrPq._pool.available.has(connStr) && PrPq._pool.available.get(connStr).length > 0 ) {
      pq._conn = PrPq._pool.available.get(connStr).pop();
      if( initSQL == null ) resolve(pq);
      else pq.query( initSQL ).then( () => resolve(pq) ).catch( (e) => reject(e) );
    }
    // at max connections - put into pending
    else if( PrPq._pool.clientCt >= PrPq._pool.MAXCLIENTS ) {
      if( !PrPq._pool.pending.has( connStr ) ) PrPq._pool.pending.set( connStr, new Array() );
      PrPq._pool.pending.get(connStr).push( { pq: pq, resolve: resolve, reject: reject } );
    }
    // create new connection 
    else {
      // inc clientCt even though it might error out, preventing race condition
      PrPq._pool.clientCt++;
      //-console.log( 'new conn: '+connStr+', clientCt='+PrPq._pool.clientCt );
      pq._conn = {
        connStr: connStr,
        libpq: new (require('libpq'))(),
      }
      pq._conn.libpq.connect( connStr, () => {
        let err = pq._conn.libpq.errorMessage();
        if( err ) return ERR( 'cannot connect: '+err, reject );
        if( initSQL == null ) resolve(pq);
        else pq.query( initSQL ).then( () => resolve(pq) ).catch( (e) => reject(e) );
      });
    }
  });
}

// internal utility function to both write errs to console and reject them if rej provided
var ERR = function( msg, rej ) {
  console.log( 'ERROR: '+msg );
  return (rej) ? rej(msg) : Promise.reject(msg);
}

// connection pool
PrPq._pool = {
  MAXCLIENTS: 10,
  clientCt: 0,
  pending: new Map(),
  available: new Map()
};
PrPq.setMAXCLIENTS = (x) => PrPq._pool.MAXCLIENTS = Math.max( PrPq._pool.clientCt, x );


/*
  conn: ( pq, connStr, callback ) => {
    // see if non-used conn available
    conns.forEach( (c) => {
      if( c.connStr == connStr && c.pq == null ) {
        c.pq = pq;
        return c;
      }
      le
    });
    let conn = null;
  }
};
*/

// global oids - from 9.4
PrPq.oids = {
   16: 'bool',
   18: 'char',
   20: 'int8',
   21: 'int2',
   23: 'int4',
   25: 'text',
   114: 'json',
   700: 'float4',
   701: 'float8',
   869: 'inet',
   1043: 'varchar',
   1082: 'date',
   1083: 'time',
   1114: 'timestamp',
   1186: 'interval',
   1700: 'numeric',
   2950: 'uuid',
   3802: 'jsonb',
   2249: 'record' };

/* global oids - storing oid / typnames for parsing results, the internal func to load them
PrPq.prototype._loadOids = () => {
  let pq = this;
  return new Promise( (resolve,reject) => {
    if( PrPq.oids !== null ) return resolve( pq );
    pq.query( "select oid, typname from pg_type where typname in( 'bool', 'char', 'int8', 'int2', 'int4', 'text', 'json', 'float4', 'float8', 'inet', 'cnet', 'mac', 'varchar', 'date', 'time', 'timestamp', 'interval', 'numeric', 'uuid', 'jsonb', 'record' )" )
    .then( (pq) => {
      PrPq.oids = new Map();
      for( let r=0; r<pq._conn.libpq.ntuples(); r++ ) {
        PrPq.oids.set( parseInt(pq._conn.libpq.getvalue(r,0)), pq._conn.libpq.getvalue(r,1) );
      }
      resolve( pq );
    })
    .catch( (e) => ERR( 'cannot load oids: '+e, reject ) )
  } );
}
*/

/* queries - return Promise, resolved when results back from server
 * query( queryStr ) - regular query
 * query( queryStr, args ) => queryParams( queryStr, args )
 * queryParams( queryStr, args ) - parameterized query
 * queryPrepared( queryStr, args ) - prepared statement query
 * execute( queryStr, args ) => queryPrepared( queryStr, args )
 */
PrPq.prototype.query = ( queryStr, args ) => {
  let pq = this;
  if( args !== undefined && args !== null ) return pq.queryParams( queryStr, args );
  //~console.log( 'query: '+queryStr );
  if( pq._conn.libpq.isBusy() ) return ERR( 'cannot query: client is busy' );
  pq._conn.libpq.clear(); 
  if( !this._conn.libpq.sendQuery( queryStr ) ) return ERR( 'cannot query: '+pq._conn.libpq.errorMessage() );
  return this._getResult();
};
PrPq.prototype.queryParams = ( queryStr, args ) => {
  let pq = this;
  //~console.log( 'queryParams: '+queryStr );
  if( pq._conn.libpq.isBusy() ) return ERR( 'cannot query: client is busy' );
  pq._conn.libpq.clear(); 
  let argss = (args instanceof Array) ? args.filter( (x) => pq._conn.libpq.escapeLiteral(x) ) : args;
  if( !pq._conn.libpq.sendQueryParams( queryStr, argss ) ) return ERR( 'query: '+pq._conn.libpq.errorMessage() );
  return pq._getResult();
};
PrPq.prototype.prepare = ( name, text, args, ignoreExists ) => {
  let pq = this;
  if( pq._conn.libpq.isBusy() ) return ERR( 'cannot prepare: client is busy' );
  pq._conn.libpq.clear(); 
  if( !pq._conn.libpq.sendPrepare( name, text, args ) ) return ERR( 'prepare: '+pq._conn.libpq.errorMessage() );
  return new Promise( (resolve,reject) => {
    pq._getResult()
    .then( () => resolve(pq) )
    .catch( (err) => {
      if( ignoreExists && (err.code == '42P05') ) return resolve( pq );
      console.log( 'prepare err: ',err );
      reject( err );
    })
  } )
};
PrPq.prototype.deallocate = ( name, ignoreNotExists ) => {
  let pq = this;
  if( pq._conn.libpq.isBusy() ) return ERR( 'cannot deallocate: client is busy' );
  pq._conn.libpq.clear(); 
  if( name===undefined || name==null ) name = 'ALL';
  if( !pq._conn.libpq.sendQuery( 'deallocate '+name ) ) return ERR( 'deallocate: '+pq._conn.libpq.errorMessage());
  return new Promise( (resolve,reject) => {
    pq._getResult()
    .then( () => resolve(pq) )
    .catch( (err) => {
      if( ignoreNotExists && (err.code == '26000') ) return resolve( pq );
      console.log( 'deallocate err: ',err );
      reject( err );
    })
  } )
};
PrPq.prototype.queryPrepared = ( name, args ) => {
  let pq = this;
  //~console.log( 'queryPrepared: '+name );
  if( pq._conn.libpq.isBusy() ) return ERR( 'cannot query: client is busy' );
  pq._conn.libpq.clear(); 
  let argss = (args instanceof Array) ? args.filter( (x) => pq._conn.libpq.escapeLiteral(x) ) : args;
  if( !pq._conn.libpq.sendQueryPrepared( name, args ) )
    return ERR( 'queryPrepared: '+pq._conn.libpq.errorMessage() );
  return pq._getResult();
};
PrPq.prototype.execute = ( name, args ) => this.queryPrepared( name, args );

PrPq.prototype._consume = () => {
  let pq = this;

  // if not busy, nothing to consume
  if( !pq._conn.libpq.isBusy() ) return Promise.resolve( pq );

  //
  return new Promise( (resolve,reject) => {
    function consume() {
      if( !pq._conn.libpq.consumeInput() ) return reject( pq._conn.libpq.errorMessage() );
      if( !pq._conn.libpq.isBusy() ) return resolve( pq );
      setTimeout( consume );
    }
    consume();
  } );
  //

  /* startReader() to listen to 'readable' events
  return new Promise( (resolve,reject) => {
    pq._conn.libpq.startReader();
    pq._conn.libpq.on('readable', function() {

      // consume input
      if( !pq._conn.libpq.consumeInput() ) return reject( pq._conn.libpq.errorMessage() );

      // note: consuming a 2nd buffer of input later...
      if(pq._conn.libpq.isBusy()) return;
     
      // release listener, stop watcher 
      //~pq._conn.libpq.removeListener('readable', onReadable);
      pq._conn.libpq.removeAllListeners('readable');
      pq._conn.libpq.stopReader();

      // done
      resolve( pq );
    });
  });
  */
}


// internal function to pull in results after query
PrPq.prototype._getResult = () => {
  let pq = this;
  return new Promise( (resolve,reject) => {
    pq._consume()
    .then( () => {
      pq._conn.libpq.getResult();
      if( pq._conn.libpq.resultStatus() == 'PGRES_FATAL_ERROR' )
        return ERR( 'result error: '+pq._conn.libpq.resultErrorMessage(), reject );
      pq._conn.libpq.stopReader();
      resolve( pq );
    })
    .catch( (e) => reject( e ) );
  });
}

// end use of this session, returning it to pool - returns Promise
PrPq.prototype.end = () => {

  let pq = this;

  // if in transaction, rollback first
  if( pq.inTrans() ) return Promise( (resolve,reject) => {
    pq.rollback().then( () => resolve( pq.end() ) )
  })

  // clear libpq
  pq._conn.libpq.clear();

  // if any pending of same connStr, give pending this conn
  if( PrPq._pool.pending.has( this._conn.connStr ) && PrPq._pool.pending.get(this._conn.connStr).length > 0 ) {
    //~console.log( 'conn end / to new pq: '+this._conn.connStr );
    let nc = PrPq._pool.pending.get(this._conn.connStr).pop();
    nc.pq._conn = this._conn;
    this._conn = null;
    nc.resolve( nc.pq );
  }

  // TODO: any pending of different connStr
  // TODO

  // nothing pending - put into connStr pool
  else {
    //~console.log( 'conn end / to pool: '+this._conn.connStr );
    if( !PrPq._pool.available.has( this._conn.connStr ) ) PrPq._pool.available.set( this._conn.connStr, new Array() );
    PrPq._pool.available.get( this._conn.connStr ).push( this._conn );
    this._conn = null;
  }

  return Promise.resolve( pq );
}

// end session and disconnect - returns Promise
PrPq.prototype.finish = () => {
  //~console.log( 'conn finish: '+this._conn.connStr );
  let pq = this;
  return Promise( (resolve,reject) =>
    pq.end().then( () => {
      pq._conn.libpq.finish();
      PrPq._pool.clientCt--;
      pq._conn = null;
      resolve( pq );
    } )
  );
}

/* result funcs - only values, no Promises here!
 * - rows() - all rows as an array
 * - row( rowNo ) - returns rowNo (or 0) row as an object
 * - col( row, col ) - returns column value
 *   = if first col is col1, then following are identical: col(0,0), col(0,'col1'), col('col1'), col()
 *   = a null value is always returned as null
 * - val( type, val ) returns a typed val if int (or oid), float, or bool
 *   = type may either be string or type's oid
 *   = unfortunately javascript does not have Time or Interval types, for example
 *   = lack of js types means there is not a 1::1 mapping between pg types and js types
 *   = also consider that most data pulled will go upstream, so not parsing if not used is good idea
 *   = NOT supported: pg arrays, records, geometry, etc
 * - rowCount() - number of rows retrieved (select, as versus DML)
 * - colCount() - number of cols in each row (select, as versus DML)
 * - affectedRows() - number of rows affected by DML insert, update or delete
 */
PrPq.prototype.rows = () => {
  let arr = [];
  for( let r=0; r<this.rowCount(); r++ ) arr.push( this.row(r) );
  return arr;
};
PrPq.prototype.row = (r) => {
  if( r === undefined || r === null ) r = 0;
  if( r > this.rowCount() ) throw( 'ERR: cannot fetch row '+c );
  let m = {};
  for( let c=0; c<this.colCount(); c++ ) {
    m[this._conn.libpq.fname(c)] = 1;
    m[this._conn.libpq.fname(c)] = this._conn.libpq.getisnull(r,c) ? null : 0;
    m[this._conn.libpq.fname(c)] = this._conn.libpq.getisnull(r,c) ? null : this.col(r,c);
  }
  return m;
}
PrPq.prototype.col = (r,c) => {
  let cl = this._conn.libpq;
  if( r === undefined ) r = 0;
  if( c === undefined ) { c=r; r=0; }
  if( typeof c == 'string' ) {
    for( let i=0; i<cl.nfields(); i++ ) {
      if( cl.fname(i)==c ) { c = i; break; }
    }
    if( typeof c == 'string' ) throw( 'unable to find column '+c );
  }
  return (cl.getisnull(r,c)) ? null : PrPq.val( cl.ftype(c), cl.getvalue(r,c) );
}
PrPq.val = function( type, val ) {

  // null is always null
  if( val === null ) return null;

  // if type passed as oid, change to string
  if( typeof type != 'string' ) type = PrPq.oids[ type ];

  // ints
  if( ['int','int8','int4','int2','oid'].indexOf(type) > -1 ) return( parseInt(val) );

  // floats
  if( ['float4','float8','number'].indexOf(type) > -1 ) return( parseFloat(val) );

  // bool
  if( ['bool'].indexOf(type) > -1 ) return( (val=='t') ? true : false );

  // json
  if( ['json','jsonb'].indexOf(type) > -1 ) return( JSON.parse( val ) );

  // everything else, including dates, keep as string!
  return val;
}

// transactions

PrPq.prototype.begin = () => {
  let pq = this;
  return Promise( (resolve,reject) => {
    if( pq.inTrans() ) pq.rollback().then( () => reject( 'begin: transaction already begun' ) );
    pq.query( 'begin' ).then( () => {
      pq._inTrans = true;
      resolve( pq );
    });
  } );
};
PrPq.prototype.inTrans = () => this._inTrans;
PrPq.prototype.commit = ( ignoreNoTransaction ) => {
  let pq = this;
  if( !pq.inTrans() && !ignoreNoTransaction ) return Promise.reject( 'commit: transaction not begun' );
  return Promise( (resolve,reject) => pq.query( 'commit' )
    .then( () => {
      pq._inTrans = false;
      resolve(pq);
    } )
    .catch( (err) => {
      if( ignoreNotExists && (err.code == '25P01') ) return resolve( pq );
      console.log( 'commit err: ',err );
      reject( err );
    }) );
};
PrPq.prototype.rollback = ( ignoreNoTransaction ) => {
  let pq = this;
  if( !pq.inTrans() && !ignoreNoTransaction ) return Promise.reject( 'rollback: transaction not begun' );
  return Promise( (resolve,reject) => pq.query( 'rollback' )
    .then( () => {
      pq._inTrans = false;
      resolve(pq);
    } )
    .catch( (err) => {
      if( ignoreNotExists && (err.code == '25P01') ) return resolve( pq );
      console.log( 'rollback err: ',err );
      reject( err );
  } ) );
};

PrPq.prototype.rowCount = () => this._conn.libpq.ntuples();
PrPq.prototype.colCount = () => this._conn.libpq.nfields();
PrPq.prototype.affectedRows = () => this._conn.libpq.cmdTuples();


/* testing

var dbParams = {
  connStr: 'port = 5432 dbname = react',
  initSQL: "select ts_app.ts_data_prepare()"
};
var testDb = function( i ) {
  let pq = new (require('./PrPq'))();
  pq.conn( dbParams.connStr, dbParams.initSQL )
  .then( () => pq.query( "select true::boolean as val" ) )
  .then( () => {
    console.log( i+'.1. rowCount: '+pq.rowCount() );
    console.log( i+'.1. row: ', pq.row(0) );
  } )
  .then( () => pq.query( "select * from ts_data" ) )
  .then( () => {
    console.log( i+'.2. rowCount: '+pq.rowCount();
  })
  .then( () => pq.queryPrepared( 'ts_data_insert', [ null, '{"name":"jonno","text":"hello test5"}' ] ) )
  .then( () => {
    console.log( i+'.3. affected rows: '+pq.affectedRows() );
  })
  .then( () => pq.query( "select count(1) from ts_data" ) )
  .then( () => {
    console.log( i+'.4. count of rows: '+pq.col() );
  })
  .then( () => {
    pq.end();
    if( i < 5 ) testDb( i+1 );
  } )
  .catch( (err) => console.log( 'ERR: '+err ) );
}

//-const arr = [1,2,3,4,5];
const arr = [1];
arr.forEach( (i) => setTimeout( testDb, Math.floor(Math.random()*1000), i ) );

*/

