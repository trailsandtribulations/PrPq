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

"use strict";

// connection pool
var pool = {
  MAXCLIENTS: 10,
  clientCt: 0,
  pending: new Map(),
  available: new Map()
};
exports.setMAXCLIENTS = (x) => pool.MAXCLIENTS = Math.max( pool.clientCt, x );
exports.getMAXCLIENTS = () => pool.MAXCLIENTS;

// global oids - from 9.4
var oids = {
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

var val = function( type, val ) {
  // null is always null
  if( val === null ) return null;
  // if type passed as oid, change to string
  if( typeof type != 'string' ) type = oids[ type ];
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

// class PrPq - empty shell until connected
exports.construct = function() {
  let connStr = null,
      trans = false,
      libpq = null,
      pq = this,

      /* connect to db
       * - connStr as documented by libpq
       * - initSQL is SQL ; separated SQL statements that will be run upon connection
       *   = eg, "set search_path = ts_app; set time zone 'UTC'"
       *   = eg, "select ts_app.initPrepare()"
      */
      conn = ( argConnStr, initSQL ) => new Promise( (resolve,reject) => {
        connStr = argConnStr;
        // have connection in pool - use it
        if( pool.available.has(connStr) && pool.available.get(connStr).length > 0 ) {
          libpq = pool.available.get(connStr).pop();
          if( initSQL == null ) resolve(pq);
          else query( initSQL ).then( () => resolve(pq) ).catch( (e) => reject(e) );
        }
        // at max connections - put into pending { pq, resolve, reject }
        else if( pool.clientCt >= pool.MAXCLIENTS ) {
          if( !pool.pending.has( connStr ) ) pool.pending.set( connStr, new Array() );
          pool.pending.get(connStr).push( { pq: pq, resolve: resolve, reject: reject } );
        }
        // create new connection 
        else {
          // inc clientCt even though it might error out, preventing race condition
          pool.clientCt++;
          //-console.log( `new conn: ${connStr}, clientCt=${pool.clientCt}, MAX=${pool.MAXCLIENTS}` );
          libpq = new (require('libpq'))();
          libpq.connect( connStr, () => {
            let err = libpq.errorMessage();
            if( err ) return ERR( 'cannot connect: '+err, reject );
            if( initSQL == null ) resolve(pq);
            else query( initSQL ).then( () => resolve(pq) ).catch( (e) => reject(e) );
          });
        }
      }),

      // internal utility function to both write errs to console and reject them if rej provided
      ERR = ( msg, rej ) => {
        console.log( 'ERROR: '+msg );
        return (rej) ? rej(msg) : Promise.reject(msg);
      },

      /* queries - return Promise, resolved when results back from server
       * query( queryStr ) - regular query
       * query( queryStr, args ) => queryParams( queryStr, args )
       * queryParams( queryStr, args ) - parameterized query
       * queryPrepared( queryStr, args ) - prepared statement query
       * execute( queryStr, args ) => queryPrepared( queryStr, args )
       */
      query = ( queryStr, args ) => {
        if( args !== undefined && args !== null ) return queryParams( queryStr, args );
        //~console.log( 'query: '+queryStr );
        if( libpq.isBusy() ) return ERR( 'cannot query: client is busy' );
        libpq.clear(); 
        if( !libpq.sendQuery( queryStr ) ) return ERR( 'cannot query: '+libpq.errorMessage() );
        return getResult();
      },
      queryParams = ( queryStr, args ) => {
        //~console.log( 'queryParams: '+queryStr );
        if( libpq.isBusy() ) return ERR( 'cannot query: client is busy' );
        libpq.clear(); 
        if( !libpq.sendQueryParams( queryStr, args ) ) return ERR( 'query: '+libpq.errorMessage() );
        return getResult();
      },
      prepare = ( name, text, args, ignoreExists ) => {
        if( libpq.isBusy() ) return ERR( 'cannot prepare: client is busy' );
        libpq.clear(); 
        if( !libpq.sendPrepare( name, text, args ) ) return ERR( 'prepare: '+libpq.errorMessage() );
        return new Promise( (resolve,reject) => {
          getResult()
          .then( () => resolve(pq) )
          .catch( (err) => {
            if( ignoreExists && (err.code == '42P05') ) return resolve( pq );
            console.log( 'prepare err: ',err );
            reject( err );
          })
        } )
      },
      deallocate = ( name, ignoreNotExists ) => {
        if( libpq.isBusy() ) return ERR( 'cannot deallocate: client is busy' );
        libpq.clear(); 
        if( name===undefined || name==null ) name = 'ALL';
        if( !libpq.sendQuery( 'deallocate '+name ) ) return ERR( 'deallocate: '+libpq.errorMessage());
        return new Promise( (resolve,reject) => {
          getResult()
          .then( () => resolve(pq) )
          .catch( (err) => {
            if( ignoreNotExists && (err.code == '26000') ) return resolve( pq );
            console.log( 'deallocate err: ',err );
            reject( err );
          })
        } )
      },
      queryPrepared = ( name, args ) => {
        //~console.log( 'queryPrepared: '+name );
        if( libpq.isBusy() ) return ERR( 'cannot query: client is busy' );
        libpq.clear(); 
        if( !libpq.sendQueryPrepared( name, args ) )
          return ERR( 'queryPrepared: '+libpq.errorMessage() );
        return getResult();
      },
      execute = ( name, args ) => queryPrepared( name, args ),

      consume = () => new Promise( (resolve,reject) => {
        function _consume() {
          if( !libpq.consumeInput() ) reject( libpq.errorMessage() );
          else if( !libpq.isBusy() ) resolve( pq );
          else setImmediate( _consume );
        };
        _consume();
      } ),

      /* startReader() to listen to 'readable' events
      consume = () => new Promise( (resolve,reject) => {
          libpq.startReader();
          libpq.on('readable', function() {

            // consume input
            if( !libpq.consumeInput() ) return reject( libpq.errorMessage() );

            // note: consuming a 2nd buffer of input later...
            if(libpq.isBusy()) return;
           
            // release listener, stop watcher 
            //~pq._conn.libpq.removeListener('readable', onReadable);
            libpq.removeAllListeners('readable');
            libpq.stopReader();

            // done
            resolve( pq );
          });
        });
      },
      */

      // internal function to pull in results after query
      getResult = () => new Promise( (resolve,reject) => {
        consume()
        .then( () => {
          libpq.getResult();
          if( libpq.resultStatus() == 'PGRES_FATAL_ERROR' ) ERR( libpq.resultErrorMessage(), reject );
          else resolve( pq );
        })
        .catch( (e) => reject( e ) );
      }),

      // transactions
      begin = () => {
        return Promise( (resolve,reject) => {
          if( trans ) rollback().then( () => reject( 'begin: transaction already begun' ) );
          query( 'begin' )
          .then( () => {
            trans = true;
            resolve( pq );
          });
        } );
      },
      commit = ( ignoreNoTransaction ) => {
        if( !trans && !ignoreNoTransaction ) return Promise.reject( 'commit: transaction not begun' );
        return Promise( (resolve,reject) => 
          query( 'commit' )
          .then( () => {
            inTrans = false;
            resolve(pq);
          } )
          .catch( (err) => {
            if( ignoreNotExists && (err.code == '25P01') ) return resolve( pq );
            console.log( 'commit err: ',err );
            reject( err );
          }) );
      },
      rollback = ( ignoreNoTransaction ) => {
        if( !trans && !ignoreNoTransaction ) return Promise.reject( 'rollback: transaction not begun' );
        return Promise( (resolve,reject) => 
          query( 'rollback' )
          .then( () => {
            inTrans = false;
            resolve(pq);
          } )
          .catch( (err) => {
            if( ignoreNotExists && (err.code == '25P01') ) return resolve( pq );
            console.log( 'rollback err: ',err );
            reject( err );
        } ) );
      },

      // end use of this session, returning it to pool - returns Promise
      end = ( arg ) => {

        // if in transaction, rollback first
        if( trans ) return Promise( (resolve,reject) => {
          rollback()
          .then( () => end() )
          .then( arg || pq )
        })

        // clear libpq
        libpq.clear();

        // if any pending of same connStr, give pending this conn
        if( pool.pending.has( connStr ) && pool.pending.get(connStr).length > 0 ) {
          //-console.log( 'conn end / to new pq: '+connStr );
          let nc = pool.pending.get(connStr).pop();
          nc.pq.libpq = libpq;
          libpq = null;
          nc.resolve( arg || nc.pq );
        }

        // TODO: any pending of different connStr
        // TODO

        // nothing pending - put into connStr pool
        else {
          //-console.log( 'conn end / to pool: '+connStr );
          if( !pool.available.has( connStr ) ) pool.available.set( connStr, new Array() );
          pool.available.get( connStr ).push( libpq );
          libpq = null;
        }

        //-console.log( `connStr=${connStr}` );
        //-console.log( 'pool.available:' );
        //-for( let k of pool.available.keys() ) console.log( `${k}: ${pool.available.get(k).length}` );
        //-console.log( 'pool.pending:' );
        //-for( let k of pool.pending.keys() ) console.log( `${k}: ${pool.pending.get(k).length}` );

        return Promise.resolve( arg || pq );
      },

      finish = () => Promise( (resolve,reject) =>
        end()
        .then( () => {
          libpq.finish();
          pool.clientCt--;
          libpq = null;
          resolve( pq );
        } )
      ), 

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
       * - inTrans() - whether in transaction
       */
      rows = () => {
        let arr = [];
        for( let r=0; r<rowCount(); r++ ) arr.push( row(r) );
        return arr;
      },
      row = (r) => {
        if( r === undefined || r === null ) r = 0;
        if( r > rowCount() ) throw( 'ERR: cannot fetch row '+c );
        let m = {};
        for( let c=0; c<colCount(); c++ ) {
          m[libpq.fname(c)] = 1;
          m[libpq.fname(c)] = libpq.getisnull(r,c) ? null : 0;
          m[libpq.fname(c)] = libpq.getisnull(r,c) ? null : col(r,c);
        }
        return m;
      },
      col = (r,c) => {
        if( r === undefined ) r = 0;
        if( c === undefined ) { c=r; r=0; }
        if( typeof c == 'string' ) {
          for( let i=0; i<libpq.nfields(); i++ ) {
            if( libpq.fname(i)==c ) { c = i; break; }
          }
          if( typeof c == 'string' ) throw( 'unable to find column '+c );
        }
        return (libpq.getisnull(r,c)) ? null : val( libpq.ftype(c), libpq.getvalue(r,c) );
      },
      rowCount = () => libpq.ntuples(),
      colCount = () => libpq.nfields(),
      affectedRows = () => libpq.cmdTuples(),
      inTrans = () => trans;

  return Object.freeze({
    conn: conn,
    query: query,
    queryParams: queryParams,
    prepare: prepare,
    deallocate: deallocate,
    queryPrepared: queryPrepared,
    execute: execute,
    begin: begin,
    commit: commit,
    rollback: rollback,
    end: end,
    finish: finish,
    rows: rows,
    row: row,
    col: col,
    rowCount: rowCount,
    colCount: colCount,
    affectedRows: affectedRows,
    inTrans: inTrans
  });

};


/* testing

var dbParams = {
  connStr: 'port = 5432 dbname = react',
  initSQL: "select ts_app.ts_data_prepare()"
};
var testDb = function( i ) {
  let PrPq = require('./PrPq');
  PrPq.setMAXCLIENTS( 20 );
  let pq = PrPq.construct();
  pq.conn( dbParams.connStr, dbParams.initSQL )
  .then( () => pq.query( "select true::boolean as val" ) )
  .then( () => {
    console.log( i+'.1. rowCount: '+pq.rowCount() );
    console.log( i+'.1. row: ', pq.row(0) );
  } )
  .then( () => pq.query( "select * from ts_data" ) )
  .then( () => {
    console.log( i+'.2. rowCount: '+pq.rowCount() );
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

