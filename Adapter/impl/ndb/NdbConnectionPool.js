/*
 Copyright (c) 2012, Oracle and/or its affiliates. All rights
 reserved.
 
 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; version 2 of
 the License.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA
 */

/*global path, build_dir, assert, unified_debug */

"use strict";

var adapter        = require(path.join(build_dir, "ndb_adapter.node")),
    ndbsession     = require("./NdbSession.js"),
    dbtablehandler = require("../common/DBTableHandler.js"),
    udebug         = unified_debug.getLogger("NdbConnectionPool.js"),
    ndb_is_initialized = false,
    proto;


function initialize_ndb() {
  if(! ndb_is_initialized) {
    adapter.ndb.ndbapi.ndb_init();                       // ndb_init()
    // adapter.ndb.util.CharsetMap_init();           // CharsetMap::init()
    unified_debug.register_client(adapter.debug);
    ndb_is_initialized = true;
  }
}


/* Load-Time Function Asserts */

assert(typeof adapter.ndb.ndbapi.Ndb_cluster_connection === 'function');
assert(typeof adapter.ndb.impl.DBSession.create === 'function');
assert(typeof adapter.ndb.impl.DBDictionary.listTables === 'function');
assert(typeof adapter.ndb.impl.DBDictionary.getTable === 'function');


/* DBConnectionPool constructor.
   IMMEDIATE.
   Does not perform any IO. 
   Throws an exception if the Properties object is invalid.
*/   
exports.DBConnectionPool = function(props) {
  udebug.log("constructor");

  initialize_ndb();
  
  this.properties = props;
  this.ndbconn = 
    new adapter.ndb.ndbapi.Ndb_cluster_connection(props.ndb_connectstring);
  this.ndbconn.set_name("nodejs");
  udebug.log("constructor returning");
};

/* NdbConnectionPool prototype 
*/
proto = {
  properties           : null,
  ndbconn              : null,
  is_connected         : false,
  dict_sess            : null,
  dictionary           : null,
};

exports.DBConnectionPool.prototype = proto;


/* Blocking connect.  
   SYNC.
   Returns true on success and false on error.
*/
proto.connectSync = function() {
  var r, nnodes;
  var db = this.properties.database;
  r = this.ndbconn.connect(this.properties.ndb_connect_retries,
                           this.properties.ndb_connect_delay,
                           this.properties.ndb_connect_verbose);
  if(r === 0) {
    nnodes = this.ndbconn.wait_until_ready(1, 1);
    if(nnodes < 0) {
      throw new Error("Timeout waiting for cluster to become ready.");
    }
    else {
      this.is_connected = true;
      if(nnodes > 0) {
        udebug.log_notice("Warning: only", nnodes, "data nodes are running.");
      }
      udebug.log_notice("Connected to cluster as node id:", this.ndbconn.node_id());
    }

    /* Get sessionImpl and session for dictionary use */
    this.dictionary = adapter.ndb.impl.DBSession.create(this.ndbconn, db);  
    this.dict_sess =  ndbsession.newDBSession(this, this.dictionary);
  }
    
  return this.is_connected;
};


/* Async connect 
*/
proto.connect = function(user_callback) {
  udebug.log("connect");
  var self = this,
      err = null;

  function onGotDictionarySession(cb_err, dsess) {
    udebug.log("connect onGotDictionarySession");
    // Got the dictionary.  Next step is the user's callback.
    if(cb_err) {
      user_callback(cb_err, null);
    }
    else {
      self.dict_sess = dsess;
      self.dictionary = self.dict_sess.impl;
      user_callback(null, self);
    }
  }

  function onReady(cb_err, nnodes) {
    // Cluster is ready.  Next step is to get the dictionary session
    udebug.log("connect() onReady nnodes =", nnodes);
    if(nnodes < 0) {
      err = new Error("Timeout waiting for cluster to become ready.");
      user_callback(err, self);
    }
    else {
      self.is_connected = true;
      if(nnodes > 0) {
        udebug.log_notice("Warning: only " + nnodes + " data nodes are running.");
      }
      udebug.log_notice("Connected to cluster as node id: " + self.ndbconn.node_id());
     }
     self.getDBSession(0, onGotDictionarySession);
  }
  
  function onConnected(cb_err, rval) {
    // Connected to NDB.  Next step is wait_until_ready().
    udebug.log("connect() onConnected rval =", rval);
    if(rval === 0) {
      assert(typeof self.ndbconn.wait_until_ready === 'function');
      self.ndbconn.wait_until_ready(1, 1, onReady);
    }
    else {
      err = new Error('NDB Connect failed ' + rval);       
      user_callback(err, self);
    }
  }
  
  // Fist step is to connect to the cluster
  self.ndbconn.connect(self.properties.ndb_connect_retries,
                       self.properties.ndb_connect_delay,
                       self.properties.ndb_connect_verbose,
                       onConnected);
};


/* DBConnection.isConnected() method.
   IMMEDIATE.
   Returns bool true/false
 */
proto.isConnected = function() {
  return this.is_connected;
};


/* closeSync()
   SYNCHRONOUS.
*/
proto.closeSync = function() {
  adapter.ndb.impl.DBSession.destroy(this.dictionary);
  this.ndbconn.delete();
};


/* getDBSession().
   ASYNC.
   Creates and opens a new DBSession.
   Users's callback receives (error, DBSession)
*/
proto.getDBSession = function(index, user_callback) {
  udebug.log("getDBSession");
  assert(this.ndbconn);
  assert(user_callback);
  var db   = this.properties.database,
      self = this;

  function private_callback(err, sessImpl) {
    udebug.log("getDBSession private_callback");

    var user_session;
    if(err) {
      user_callback(err, null);
    }
    else {  
      user_session = ndbsession.newDBSession(self, sessImpl);
      user_callback(null, user_session);
    }
  }

  adapter.ndb.impl.DBSession.create(this.ndbconn, db, private_callback);
};


/** List all tables in the schema
  * ASYNC
  * 
  * listTables(databaseName, dbSession, callback(error, array));
  */
proto.listTables = function(databaseName, dbSession, user_callback) {
  udebug.log("listTables");
  assert(databaseName && user_callback);
  var dictSessionImpl = dbSession ? dbSession.impl : this.dictionary;
  adapter.ndb.impl.DBDictionary.listTables(dictSessionImpl, databaseName, user_callback);
};


/** Fetch metadata for a table
  * ASYNC
  * 
  * getTableMetadata(databaseName, tableName, dbSession, callback(error, TableMetadata));
  */
proto.getTableMetadata = function(dbname, tabname, dbSession, user_callback) {
  udebug.log("getTableMetadata");
  assert(dbname && tabname && user_callback);
  var dictSessionImpl = dbSession ? dbSession.impl : this.dictionary;
  // TODO: Wrap the NdbError in a large explicit error message db.tbl not in ndb engine
  udebug.log_detail(dictSessionImpl, dbname, tabname, user_callback);
  adapter.ndb.impl.DBDictionary.getTable(dictSessionImpl, dbname, tabname, user_callback);
};


/* createDBTableHandler(tableMetadata, apiMapping)
   IMMEDIATE
   Creates and returns a DBTableHandler for table and mapping
*/
proto.createDBTableHandler = function(tableMetadata, apiMapping) { 
  udebug.log("createDBTableHandler", tableMetadata.name);
  var handler;
  handler = new dbtablehandler.DBTableHandler(tableMetadata, apiMapping);
  /* TODO: If the mapping is not a default mapping, then the DBTableHandler
     needs to be annotated with some Records */
  return handler;
};
