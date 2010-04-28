var sys = require('sys');
var _ = require('underscore')._
var events = require('events')

var no_op_fn = function() {}
var logger  = { isDebugEnabled: function() {return false} };
['debug', 'info', 'error', 'warn', 'fatal'].forEach(function(f) { logger[f] = no_op_fn }); 

var keyspaces = {};

function func_for_column_family(keyspace, column_family, func) {
  return function() {
    var args = [keyspace, column_family];
    args = args.concat(Array.prototype.slice.call(arguments));
    func.apply(this, args);
  }
}

function find_objects() {
  
  var keyspace, column_family, key_spec, super_column_name, column_name_or_predicate, event_listeners;
  var key, keys, range, column_name, predicate;
  keyspace = arguments[0];
  column_family = arguments[1];
  var cf = get_column_family(keyspace, column_family)
  event_listeners = arguments[arguments.length-1];
  var penultimate_arg = arguments[arguments.length-2];
  if (typeof penultimate_arg == 'object' && arguments.length > 4) {
    column_name_or_predicate = penultimate_arg;
    handle_id_args.apply(this, Array.prototype.slice.call(arguments, 2, arguments.length-2));
  } else {
    handle_id_args.apply(this, Array.prototype.slice.call(arguments, 2, arguments.length-1));
  }
  function handle_id_args() {
    key_spec = arguments[0];
    if (cf.type == "Super") {
      super_column_name = arguments[1];
      if (arguments.length > 2) column_name_or_predicate = arguments[2];            
    } else {
      if (arguments.length > 1) column_name_or_predicate = arguments[1];
    }
  }
  // handle_id_args.apply(this, Array.prototype.slice.call(arguments, 2, arguments.length-1));  
  
  if (key_spec.constructor.name == 'Object') {
    range = key_spec;
  } else if (key_spec.constructor.name == 'Array') {
    keys = key_spec;
  } else {
    key = key_spec;
  }

  if (!column_name_or_predicate) {
    if (super_column_name && cf.subcolumn_names) {
      predicate = {column_names: cf.subcolumn_names}
    } else if (!super_column_name && cf.column_names) {
      predicate = {column_names:cf.column_names}
    } else {
      throw "Must specify a column name or predicate."
    }
  } else if (typeof column_name_or_predicate == 'object') {
    predicate = column_name_or_predicate;
  } else {
    column_name = column_name_or_predicate;
    predicate = {column_names:[column_name]};
  }

  // logger.debug("find_objects - keyspace: " + keyspace + ",column_family:" + column_family + 
  //              ",key:" + key + ",keys:" + sys.inspect(keys) + ",range:" + sys.inspect(range) +  
  //              ",super_column_name:" + super_column_name + ",column_name:" + column_name + 
  //              ",predicate:" + sys.inspect(predicate));
  var cassandra = keyspaces[keyspace].cassandra,
      column_parent = {column_family: column_family},
      get_request;
  if (super_column_name) column_parent.super_column = super_column_name;
  if (key) {
    get_request = cassandra.create_request("get_slice",
      {keyspace: keyspace, key:key, column_parent: column_parent, 
       predicate: predicate, consistency_level: ConsistencyLevel.ONE})
  } else if (range) {
    get_request = cassandra.create_request("get_range_slices",
      {keyspace:keyspace, column_parent:column_parent, predicate:predicate, 
       range: range, consistency_level: ConsistencyLevel.ONE})
  } else { // must be keys
    get_request = cassandra.create_request("multiget_slice",
      {keyspace:keyspace, column_parent:column_parent, predicate:predicate, 
       keys: keys, consistency_level: ConsistencyLevel.ONE})
  }
  
  function insert_callback(o) {
    if (column_name) {
      insert_after_callbacks(keyspace, column_family, event_listeners, 
                             "after_find_column", o);    
    } else if (super_column_name) {
      insert_after_callbacks(keyspace, column_family, event_listeners, 
                             "after_find_super_column", o);    
    } else {
      insert_after_callbacks(keyspace, column_family, event_listeners, 
                             "after_find_row", o);    

    }    
  }
  
  get_request.addListener("success", function(result) {
    var object_result, timestamp;
    if (key) { // list of columns or super columns
      if (result.length > 0) {
        if (column_name) {
          columns = result[0].value
          timestamp = result[0].timestamp
        } else {
          columns = result;
        }
        object_result = create_mem_object(keyspace, column_family, key, 
                          super_column_name, column_name, columns, timestamp);
        object_result.update_last_saved();
        insert_callback(object_result);
      }                                              
    } else if (range) { // list of keyslices 
      object_result = [];
      result.forEach(function(ks){
        if (column_name) {
          columns = ks.columns[0].value
          timestamp = ks.columns[0].timestamp
        } else {
          columns = ks.columns;
        }
        if (columns.length > 0) {
          var o = create_mem_object(keyspace, column_family, ks.key, 
            super_column_name, column_name, columns, timestamp)
          o.update_last_saved();
          insert_callback(o);
          object_result.push(o);
        }
      })
    } else { // hash of lists of columns or super columns
      object_result = {}
      for ( var k in result ) {
        var columns = result[k];
        if (column_name) {
          columns = columns[0].value
          timestamp = columns[0].timestamp
        }
        if (columns.length > 0) {
          object_result[k] = create_mem_object(keyspace, column_family, ks.key, 
                               super_column_name, column_name, columns, timestamp)
          object_result[k].update_last_saved();
          insert_callback(object_result[k]);
        }
      }
    }
    if (object_result) {
      if (logger.isDebugEnabled()) {
        logger.debug("Found object(s) in " + keyspace + "." + column_family + 
                     ": " + sys.inspect(object_result));
      }
      if (event_listeners.success) event_listeners.success(object_result);
    } else {
      if (event_listeners.not_found) event_listeners.not_found();
    }
  })
  get_request.addListener("error", function(mess) {
    var error_mess = "Error finding object(s) in " + keyspace + "." + 
                     column_family + ": " + mess;
    logger.error(error_mess);
    if (event_listeners.error) event_listeners.error(error_mess);
  })
  
  get_request.send();
}

function remove_object(keyspace, column_family, key, super_column_name, column_name, event_listeners) {
  var cassandra = keyspaces[keyspace].cassandra;
  var cf = get_column_family(keyspace, column_family)
  var column_path = {column_family: column_family}
  if (super_column_name) column_path.super_column = super_column_name;
  if (column_name) column_path.column = column_name;
  var request = cassandra.create_request("remove", {
    keyspace: keyspace, column_path: column_path, timestamp: "auto"
  })
  if (event_listeners.success) {
    request.addListener("success", event_listeners.success);
  }
  request.addListener("error", function(mess) {
    var error_mess = "Error trying to remove " + 
      path_s(keyspace, column_family, key, super_column_name, column_name) + 
      ":" + mess;
    if (event_listeners.error) event_listeners.error(error_mess);
  });
  request.send();
}

function save_row_object(keyspace, column_family, o, event_listeners, delete_missing_columns) {
  if (typeof delete_missing_columns == 'undefined') {
    delete_missing_columns = true
  }      
  insert_after_callbacks(keyspace, column_family, event_listeners, 
                         "after_save_row", o);
  save_object(keyspace, column_family, o.key, o, true, event_listeners, 
    function() {
      return mutations_for_save_row_object(keyspace, column_family, o, delete_missing_columns);
    },
    function(new_timestamp) {
      var cf = get_column_family(keyspace, column_family)
      if (cf.type == "Super") {
        if ( cf.column_names ) {
          cf.column_names.forEach(function (col_name) {
            handle_new_timestamp_for_super_column_object(keyspace, column_family, new_timestamp, o[col_name]);
          })
        } else {
          o.columns.forEach(function(col) {
            handle_new_timestamp_for_super_column_object(keyspace, column_family, new_timestamp, col);
          });
        }
      } else {
        if ( cf.column_names ) {
          cf.column_names.forEach(function (col_name) {
            o.timestamps[col_name] = new_timestamp;
          })
        } else {
          o.columns.forEach(function(col) {
            col.timestamp = new_timestamp;
          });
        }
      } 
    }, true)
}

function mutations_for_save_row_object(keyspace, column_family, o, delete_missing_columns) {
  var mutations;
  var cf = get_column_family(keyspace, column_family)
  if (cf.type == "Super") {
    if (cf.column_names) {
      mutations = _.reduce(cf.column_names, [], function(memo, col_name) {
        return memo.concat(mutations_for_save_super_column_object(keyspace, 
                             column_family, o[col_name], delete_missing_columns));
      })
    } else {
      mutations = _.reduce(o.columns, [], function(memo, col) {
        return memo.concat(mutations_for_save_super_column_object(keyspace, 
                             column_family, col, delete_missing_columns));
      })
    }        
  } else if (cf.column_value_type && cf.column_value_type == 'json') { 
    if (cf.column_names) {
      mutations = _.reduce(cf.column_names, [], function(memo, name) {
        if (o[name]) memo.push(column_for_save_column_object(o[name]));
        return memo;
      })          
    } else {
      mutations = _.map(o.columns, function(col) {
        return column_for_save_column_object(col);
      })      
    }
  } else {
    if (cf.column_names) {
      mutations = _.reduce(cf.column_names, [], function(memo, name) {
        if (o[name]) memo.push({name:name, value: o[name], timestamp: "auto"}); 
        return memo;
      })
    } else {
      mutations = _.map(o.columns, function(col) { 
        return {name:col.name, value: col.value, timestamp: "auto"};
      })
    }        
  }
  if (delete_missing_columns && cf.column_names && cf.type != "Super") {
    var deletions = _.reduce(cf.column_names, [], function(memo, name) {
      if (!o[name] && o.timestamps && o.timestamps[name]) {
        memo.push({
              timestamp: o.timestamps[name], 
              predicate: {column_names: [name]}
             })
      } 
      return memo;
    })
    mutations = mutations.concat(deletions);
  }     
  return mutations;
  
}

function save_super_column_object(keyspace, column_family, key, o, event_listeners, delete_missing_columns) {
  insert_after_callbacks(keyspace, column_family, event_listeners, 
                         "after_save_super_column", o);
  save_object(keyspace, column_family, key, o, true, event_listeners, 
    function() {
      return mutations_for_save_super_column_object(keyspace, column_family, o, delete_missing_columns);
    },
    function(new_timestamp) {
      handle_new_timestamp_for_super_column_object(keyspace, column_family, new_timestamp, o)
    }, true);  
}

function handle_new_timestamp_for_super_column_object(keyspace, column_family, new_timestamp, o) {
  var cf = get_column_family(keyspace, column_family)
  if (cf.subcolumn_names) {
    cf.subcolumn_names.forEach(function (col_name) {
      o.timestamps[col_name] = new_timestamp;
    })
  } else {
    o.columns.forEach(function(col) {
      col.timestamp = new_timestamp        
    });
  }
}

function mutations_for_save_super_column_object(keyspace, column_family, o, delete_missing_columns) {
  if (typeof delete_missing_columns == 'undefined') {
    delete_missing_columns = true
  }      
  var mutations, insert_columns;
  var cf = get_column_family(keyspace, column_family)
  if (cf.subcolumn_value_type == 'json') {
    if (cf.subcolumn_names) {
      insert_columns = _.reduce(cf.subcolumn_names, [], function(memo, name) {
        if (o[name]) memo.push(column_for_save_column_object(o));
        return memo;
      })
    } else {
      insert_columns = _.map(o.columns, function(col) {
        return column_for_save_column_object(col);
      })      
    }
  } else {
    if (cf.subcolumn_names) {
      insert_columns = _.reduce(cf.subcolumn_names, [], function(memo, name) {
        if (o[name]) memo.push({name:name, value: o[name], timestamp: "auto"});
        return memo;
      })
    } else {
      insert_columns = _.map(o.columns, function(col) { 
        return {name:col.name, value: col.value, timestamp: "auto"};
      })
    }
  }
  mutations = [{name: o._name, columns: insert_columns}];
  if (delete_missing_columns && cf.subcolumn_names) {
    var deletions = _.reduce(cf.subcolumn_names, [], function(memo, name) {
      if (o[name] && o.timestamps && o.timestamps[name]) {
        memo.push({
                     timestamp: o.timestamps[name], 
                     super_column: o._name, 
                     predicate: {column_names: [name]}
                    }) 
      }
      return memo;
    })
    mutations = mutations.concat(deletions);
  }  
  return mutations;  
}

function save_column_object(keyspace, column_family, key, super_column_name, o, event_listeners) {
  insert_after_callbacks(keyspace, column_family, event_listeners, 
                         "after_save_column", o);
  save_object(keyspace, column_family, key, o, true, event_listeners, 
    function() {
      return mutations_for_save_column_object(super_column_name, o);
    }, null, true);
}

function mutations_for_save_column_object(super_column_name, o) {
  if (super_column_name) {
    return [{name:super_column_name, columns:[column_for_save_column_object(o)]}];
  } else {
    return [column_for_save_column_object(o)];
  }  
}

function column_for_save_column_object(o) {
  var name = o._name;
  var last_saved = o._last_saved;
  delete o._name;
  delete o._last_saved;
  var json = JSON.stringify(o);
  o._name = name;
  o._last_saved = last_saved;
  return {name: o._name, value: json, timestamp: "auto"};  
}

function destroy_row_object(keyspace, column_family, o, event_listeners) {
  insert_after_callbacks(keyspace, column_family, event_listeners, 
                         "after_destroy_row", o);
  save_object(keyspace, column_family, o.key, o, false, event_listeners, 
    function() {
      return mutations_for_destroy_row_object(keyspace, column_family, o);
    })
}

function mutations_for_destroy_row_object(keyspace, column_family, o) {
  var mutations;
  var cf = get_column_family(keyspace, column_family)
  if (cf.type == "Super") {
    if (cf.column_names) {
      mutations = _.reduce(cf.column_names, [], function(memo, col_name) {
        return memo.concat(mutations_for_destroy_super_column_object(keyspace, 
                             column_family, o[col_name]));
      })
    } else {
      mutations = _.reduce(o.columns, [], function(memo, col) {
        return memo.concat(mutations_for_destroy_super_column_object(keyspace, 
                             column_family, col));
      })
    }        
  } else if (cf.column_value_type && cf.column_value_type == 'json') { 
    if (cf.column_names) {
      mutations = _.reduce(cf.column_names, [], function(memo, name) {
        if (o[name] && o.timestamps[name]) 
          memo.push(mutation_for_destroy_column_object(null, o[name]));
        return memo;
      })          
    } else {
      mutations = _.reduce(o.columns, [], function(memo, col) {
        if (col.timestamp)
          memo.push(mutation_for_destroy_column_object(null, col));
        return memo;
      })      
    }
  } else {
    if (cf.column_names) {
      mutations = _.reduce(cf.column_names, [], function(memo, name) {
        timestamp = o.timestamps[name]
        if (o[name] && timestamp) 
          memo.push({timestamp: timestamp, predicate: {column_names:[name]}}); 
        return memo;
      })
    } else {
      mutations = _.reduce(o.columns, [], function(memo, col) { 
        if (col.timestamp)
          memo.push({timestamp: col.timestamp, predicate: {column_names: [col.name]}});
        return memo;
      })
    }        
  }
  return merge_mutations_by_timestamp(mutations);
}

function destroy_super_column_object(keyspace, column_family, key, o, event_listeners) {
  insert_after_callbacks(keyspace, column_family, event_listeners, 
                         "after_destroy_super_column", o);
  save_object(keyspace, column_family, key, o, false, event_listeners, 
    function() {
      return mutations_for_destroy_super_column_object(keyspace, column_family, o);
    },
    function(new_timestamp) {
      if (o.timestamps) {
        for (var k in o.timestamps) {
          o.timestamps[k] = new_timestamp;
        }
      } else {
        o.columns.forEach(function(col) {col.timestamp = new_timestamp});
      }
    })  
}

function mutations_for_destroy_super_column_object(keyspace, column_family, o) {
  var mutations;
  var cf = get_column_family(keyspace, column_family)
  if (cf.subcolumn_value_type == 'json') {
    if (cf.subcolumn_names) {
      mutations = _.reduce(cf.subcolumn_names, [], function(memo, name) {
        if (o[name] && o.timestamps[name]) 
          memo.push(mutation_for_destroy_column_object(o._name, 
                      o[name]));
        return memo;
      })
    } else {
      mutations = _.reduce(o.columns, [], function(memo, col) {
        if (col.timestamp)
          memo.push(mutation_for_destroy_column_object(o._name, col));
        return memo;
      })      
    }
  } else {
    if (cf.subcolumn_names) {
      mutations = _.reduce(cf.subcolumn_names, [], function(memo, name) {
        if (o[name] && o.timestamps[name]) { 
          memo.push({timestamp:o.timestamps[name], 
                    super_column: o._name, 
                    predicate: {column_names: [name]}});
        }
        return memo;
      })
    } else {
      mutations = _.reduce(o.columns, [], function(memo, col) { 
        if (col.timestamp) { 
          memo.push({timestamp:col.timestamp, 
                     super_column: o._name, 
                     predicate: {column_names: [col._name]}});
        }
        return memo;
      })
    }
  }
  return merge_mutations_by_timestamp(mutations);
}

function destroy_column_object(keyspace, column_family, key, super_column_name, o, event_listeners) {
  insert_after_callbacks(keyspace, column_family, event_listeners, 
                         "after_destroy_column", o);
  save_object(keyspace, column_family, key, o, false, event_listeners, 
    function() { return [mutation_for_destroy_column_object(super_column_name, o)]; })  
}

function mutation_for_destroy_column_object(super_column_name, o) {
  if (!o.timestamp) throw "Cannot destroy a column object without a timestamp!";
  var mut = {timestamp: o.timestamp, predicate: {column_names: [o._name]}}
  if (super_column_name) mut.super_column = super_column_name;
  return mut;
}

function save_object(keyspace, column_family, key, o, auto_generate_ids, event_listeners, mutations_func, timestamp_func, update_last_saved) {
  // logger.debug("save_object - keyspace: " + keyspace + ",column_family:" + column_family + 
  //          ",key:" + key + ",o:" + sys.inspect(o));
           
  var cassandra = keyspaces[keyspace].cassandra;

  if ( key ) {
    now_have_key();
  } else if (!auto_generate_ids) {
    throw "Cannot save/destroy an object without a key."
  } else {
    var get_uuids = cassandra.create_request("get_uuids")
    get_uuids.addListener("success", function(result) {
      o.key = key = result[0];
      now_have_key();
    })
    get_uuids.addListener("error", function(mess) {
      var error_mess = "Could not get UUID for key when attempting to save object under " + 
        path_s(keyspace, column_family, key) + ':'  + mess;
      logger.error(error_mess);
      if (event_listeners.error) event_listeners.error(error_mess)      
    });
    get_uuids.send();
  } 
  
  function now_have_key() {
    if ( o.id ) {
      now_have_id();
    } else if (!auto_generate_ids) {
      throw "Cannot save/destroy an object without a _name."
    } else {
      var get_uuids = cassandra.create_request("get_uuids")
      get_uuids.addListener("success", function(result) {
        o._name = result[0];
        now_have_id();
      })
      get_uuids.addListener("error", function(mess) {
        var error_mess = "Could not get UUID for id when attempting to save/destroy object under " +
                          path_s(keyspace, column_family, key) + ': ' + mess;
        logger.error(error_mess);
        if (event_listeners.error) event_listeners.error(error_mess)      
      });
      get_uuids.send();
    }     
  }
    
  function now_have_id() {
    if (update_last_saved && o.before_save_callbacks &&
        o.before_save_callbacks.length > 0 ) {
      var previous_version = o._last_saved;
      call_callbacks(o.before_save_callbacks, o, function() {
        do_save();
      }, previous_version);
    } else {
      do_save();
    }
  }
  
  function do_save() {
    var mut_map = {};
    mut_map[key] = {};
    var mutations = mut_map[key][column_family] = mutations_func();
    if (!mutations || mutations.length < 1) throw "Nothing to save/destroy!";
    var mutate_request = cassandra.create_request("batch_mutate",
      {keyspace: keyspace, mutation_map: mut_map, 
       consistency_level: ConsistencyLevel.ONE})
    mutate_request.addListener("success", function(result) {
      if (timestamp_func) timestamp_func(result);
      if (update_last_saved) o.update_last_saved();
      if (event_listeners.success) event_listeners.success(o.id);
    })
    mutate_request.addListener("error", function(mess) {
      var error_mess = "Error saving/destroying object under '" + 
                        path_s(keyspace, column_family, key) + ": " + mess;
      logger.error(error_mess);
      if (event_listeners.error) event_listeners.error(error_mess);
    })
    mutate_request.send()
  }
}

function merge_mutations_by_timestamp(mutations) {
  return _.reduce(mutations, [], function(memo, mut) {
    var merge_mut = _.detect(memo, function(memo_mut) {
      return mut.timestamp == memo_mut.timestamp;
    })
    if (merge_mut) {
      var merged_names = merge_mut.
                           predicate.
                           column_names.concat(mut.predicate.column_names)
      merge_mut.predicate.column_names = merged_names;
    } else {
      memo.push(mut);
    }
    return memo;
  })
}

function create_mem_object(keyspace, column_family, key, super_column_name, column_name, columns, timestamp) {
  var o = {};
  if ( logger.isDebugEnabled() ) 
    var path = path_s(keyspace, column_family, key, super_column_name, column_name);
  var cf = get_column_family(keyspace, column_family);
  if (column_name) { // value under a column name
    var subcolumn_value_type = cf.subcolumn_value_type
    var column_value_type = cf.column_value_type
    if ((super_column_name && subcolumn_value_type && subcolumn_value_type  == 'json')
         ||
         (!super_column_name && column_value_type && column_value_type == 'json')) {
      if ( logger.isDebugEnabled() )
        logger.debug("Creating json value " + sys.inspect(columns) + " under " + path);
      o = eval('(' + columns + ')')
      o.timestamp = timestamp;
    } else {
      if ( logger.isDebugEnabled() )
        logger.debug("Returning value " + sys.inspect(columns) + " under " + path);
      o = columns;
    }      
  } else if ( super_column_name ) { // object under a super column
    if ( cf.subcolumn_names ) {
      if ( logger.isDebugEnabled() ) {
        logger.debug("Creating super column object with subcolumn_names " + 
                     sys.inspect(cf.subcolumn_names) + " under " + path);
      }
      o.timestamps = {}
      columns.forEach(function(col) {
        o[col.name] = create_mem_object(keyspace, column_family, key, 
          super_column_name, col.name, col.value, col.timestamp)
        o.timestamps[col.name] = col.timestamp;
      })
    } else {
      if ( logger.isDebugEnabled() ) {
        logger.debug("Creating super column object with dynamic column names under " + path);
      }
      o.columns = [];
      columns.forEach(function(col) {
        var value = create_mem_object(keyspace, column_family, key, 
                        super_column_name, col.name, col.value, col.timestamp)
        if (typeof value == 'object') {
          o.columns.push(value);
        } else {
          o.columns.push({name: col.name, value: value});          
        }
      })
    }
  } else  { // object under a key
    if (cf.type == "Super") { 
      if ( cf.column_names ) {
        if ( logger.isDebugEnabled() ) {
          logger.debug("Creating row object with super column names " +
            sys.inspect(cf.column_names) + " under " + path);
        }
        columns.forEach(function(col) {
          o[col.name] = create_mem_object(keyspace, column_family, key, col.name, 
            null, col.columns);
          o.timestamps[col.name] = col.timestamp;
        })
      } else {
        o.columns = []
        if ( logger.isDebugEnabled() ) {
          logger.debug("Creating row object with dynamic super column names under " + path);
        }
        columns.forEach(function(col) {
          o.columns.push(create_mem_object(keyspace, column_family, key, col.name, 
                           null, col.columns));
        })
      }
    } else {
      if ( cf.column_names ) {
        o.timestamps = {}
        if ( logger.isDebugEnabled() ) {
          logger.debug("Creating row object with column names " +
            sys.inspect(cf.column_names) + " under " + path);
        }        
        columns.forEach(function(col) {
          o[col.name] = create_mem_object(keyspace, column_family, key, null, 
            col.name, col.value, col.timestamp);
          o.timestamps[col.name] = col.timestamp;            
        })
      } else {
        o.columns = []
        if ( logger.isDebugEnabled() ) {
          logger.debug("Creating row object with dynamic column names under " + path);
        }
        columns.forEach(function(col) {
          value = create_mem_object(keyspace, column_family, key, null, 
                    col.name, col.value, col.timestamp)
          if (typeof value == 'object') {
            o.columns.push(value);
          } else {
            o.columns.push({name: col.name, value: value, timestamp: col.timestamp});
          }                    
        })
      }
    }
  }
  
  activate_object(keyspace, column_family, key, super_column_name, 
                        column_name, o);
  return o;
}

function activate_object(keyspace, column_family, key, super_column_name, column_name, o) {
  // logger.info("--- activate_object path: " + path_s(keyspace, column_family, key, super_column_name, column_name))  
  
  function this_key() { return this.key; }
  function this_name() { return this._name; }
  o._last_saved = null;
  o.update_last_saved = function() {

    function copy(thing) {
      var a_copy;
      if (typeof thing == "object") {
        a_copy = {}
        for (var k in thing) {
          a_copy[k] = copy(thing[k]);
        }
      } else {
        a_copy = thing;
      }
      return a_copy;
    }
    
    this._last_saved = {};
    Object.defineProperty(this._last_saved, "id", { 
      get: (super_column_name || column_name ? this_name : this_key) 
    });
    for (var k in this) {
      if (k == "_last_saved" || k == "update_last_saved") continue;
      var val = this[k];
      if (!val) {
        this._last_saved[k] = val;
      } else if ( Array.isArray(val) ) {
        this._last_saved[k] = [];
        var that = this;
        val.forEach(function(item) {
          if (item._last_saved !== undefined) {
            item.update_last_saved();
            that._last_saved[k].push(item._last_saved);
          } else {
            that._last_saved[k].push(copy(item));
          }
        });
      } else if (val._last_saved != undefined) { 
        val.update_last_saved();
        this._last_saved[k] = val._last_saved;
      } else {
        this._last_saved[k] = copy(val);
      }
    }
    return this;
  }    
  var cf = get_column_family(keyspace, column_family);
  if (column_name) {
    if (typeof o == 'object') {
      o._name = column_name;
      Object.defineProperty(o, "id", { get: this_name });
      Object.defineProperty(o, "key", { get: this_key });
      Object.defineProperty(o, "before_save_callbacks", { 
        get: function() { return cf.callbacks.before_save_column;} 
      });
      o.get_super_column_name = function() { return super_column_name};
      o.save = function(event_listeners, delete_missing_columns) {
        save_column_object(keyspace, column_family, key, super_column_name, this, event_listeners);
      }
      o.destroy = function(event_listeners) {
        destroy_column_object(keyspace, column_family, key, super_column_name, this, event_listeners);
      }
    }
  } else if (super_column_name) {
    o._name = super_column_name;
    Object.defineProperty(o, "id", { get: this_name });
    Object.defineProperty(o, "key", { get: this_key });
    Object.defineProperty(o, "before_save_callbacks", { 
      get: function() { return cf.callbacks.before_save_super_column;} 
    });
    if (!o.columns && !cf.subcolumn_names) o.columns = [];
    if (!o.timestamps && cf.subcolumn_names) o.timestamps = {};
    o.save = function(event_listeners, delete_missing_columns) {
      save_super_column_object(keyspace, column_family, key, this, 
        event_listeners, delete_missing_columns);
    }  
    o.destroy = function(event_listeners) {
      destroy_super_column_object(keyspace, column_family, key, this, event_listeners);
    }
  } else {
    o.key = key;
    if (!o.columns && !cf.column_names) o.columns = [];      
    if (!o.timestamps && cf.column_names) o.timestamps = {};
    Object.defineProperty(o, "id", { get: this_key });
    Object.defineProperty(o, "before_save_callbacks", { 
      get: function() { return cf.callbacks.before_save_row;} 
    });
    o.save = function(event_listeners) {
      save_row_object(keyspace, column_family, this, event_listeners);
    }
    o.destroy = function(event_listeners, totally) {
      if (totally) {
        get_column_family(keyspace, column_family).remove(this.key, event_listeners);
      } else {
        destroy_row_object(keyspace, column_family, this, event_listeners);        
      }
    }    
  }
  return o;
}

function insert_after_callbacks(keyspace, column_family, event_listeners, callback_name, o) {
  var cf = get_column_family(keyspace, column_family)
  var callbacks = cf.callbacks[callback_name]
  if (!callbacks || callbacks.length < 1) 
       return;
  var old_success = event_listeners.success;
  var previous_version = o._last_saved;
  event_listeners.success = function(result) {
    call_callbacks(callbacks, o, old_success, previous_version);    
  }  
}

function path_s(keyspace, column_family, key, super_column_name, column_name) {
  var s = '/' + keyspace + '/' + column_family;
  if (key) {
    s += '/' + key
  }
  if (super_column_name) {
    s += '/' + super_column_name
  }
  if (column_name) {
    s += '/' + column_name
  }
  return s;
}

function call_callbacks(callbacks, o, finish, previous_version, i) {
  i = i || 0;
  var cb = callbacks[i];
  cb.call(o, {
    success: function() {
      if (i < callbacks.length - 1) 
        call_callbacks(callbacks, o, finish, previous_version, i+1);
      else finish();
    },
    error: function(mess) {
      var error_mess = "Error in " + callback_name + " callback " + mess
      event_listeners.error(error_mess);
    }
  }, previous_version)
}


exports.initialize_keyspaces = function(ks_configs) {
  ks_configs.forEach(function(ks_config) { 
    var ks = keyspaces[ks_config.name] = {}
    ks.name = ks_config.name
    ks.cassandra = require('cassandra-node-client').create(
                     ks_config.cassandra_port, ks_config.cassandra_host, logger)
    ks.column_families = {}
    for (var cf_name in ks_config.column_families) {
      var cf_config =  ks_config.column_families[cf_name];
      var cf = ks.column_families[cf_name] = {
        name: cf_name,
        type: cf_config.type,
        column_names: cf_config.column_names,
        column_value_type: cf_config.column_value_type,
        subcolumn_names: cf_config.subcolumn_names,
        subcolumn_value_type: cf_config.subcolumn_value_type,
        callbacks: cf_config.callbacks || {}
      };
      cf.add_callback = function(name, func) {
        var cb_list = this.callbacks[name];
        if (!cb_list) cb_list = this.callbacks[name] = [];
        cb_list.push(func);         
      }
      cf.new_object = function() {
        // logger.info("new_object - arguments: " + sys.inspect(arguments));

        var key, super_column_name, column_name, init_cols;
        var last_arg = arguments[arguments.length-1];
        if (typeof last_arg == 'object' &&
            (last_arg.constructor.name == 'Object' || last_arg.constructor.name == 'Array')) {
          init_cols = last_arg;
          handle_id_args.apply(this, Array.prototype.slice.call(arguments, 0, arguments.length-1));
        } else {
          handle_id_args.apply(this, Array.prototype.slice.call(arguments, 0, arguments.length));
        }
        function handle_id_args() {
          key = arguments[0];
          if (this.type == "Super") {
            super_column_name = arguments[1];
            column_name = arguments[2];            
          } else {
            column_name = arguments[1];
          }
        }
                     
        if ( column_name ) columns = '{}'
        else columns = []
        // logger.info("new_object - keyspace: " + ks_config.name + ",column_family:" + this.name + 
        //          ",key:" + key + ",super_column_name:" + super_column_name + ",column_name:" + this.column_name +
        //          ",init_cols:" + sys.inspect(init_cols) + ",columns:" + sys.inspect(columns));
        var mem_obj = create_mem_object(ks_config.name, this.name, key, super_column_name,
                          column_name, columns);
                          
        if (init_cols) { 
          if (init_cols.constructor.name == 'Object') {                                                
            var val_type;
            // ugly but effective way to get the type of values in the hash
            for (var k in init_cols) {
              val_type = typeof init_cols[k]
              break; 
            }          
            // can initialize with a hash if this is:
            // - a column object, or
            // - a row object with fixed column names or json column value type, or
            // - a super column object with fixed subcolumn names or json column value type
            if (column_name || val_type != 'object') {
              for (var name in init_cols) {
                mem_obj[name] = init_cols[name];
              }
            } else if ( !super_column_name && 
                        (this.column_names || this.column_value_type == 'json') ) {
              for (var name in init_cols) {
                mem_obj[name] = this.new_object(key, name, init_cols[name]);
              }
            } else if ( super_column_name && 
                        (this.subcolumn_names || this.subcolumn_value_type == 'json') ) {
              for (var name in init_cols) {
                mem_obj[name] = this.new_object(key, super_column_name, name, init_cols[name]);
              }
            } else {
              throw "Cannot use a hash to initialize this object."
            }
          } else if (init_cols.constructor.name == 'Array') {
            // can initialize with an array if this is:
            // - not a column object, *and*
            // - a row object with dynamic column names, or
            // - a super column object with dynamic column names.
            if (!column_name && !super_column_name && !this.column_names ) {
              var that = this;
              if (this.type == "Super") {
                init_cols.forEach(function(col) {
                  if (that.subcolumn_names) {
                    mem_obj.columns.push(that.new_object(key, col._name, col));                                        
                  } else {
                    mem_obj.columns.push(that.new_object(key, col._name, col.columns));                    
                  }
                })
              } else if (this.column_value_type == "json") {
                init_cols.forEach(function(col) {
                  mem_obj.columns.push(that.new_object(key, col._name, col));
                })
              } else {
                init_cols.forEach(function(col) {                
                  mem_obj.columns.push(col);
                })
              }
            } else if (!column_name && super_column_name && !this.subcolumn_names) {
              var that = this;
              if (this.subcolumn_value_type == "json") {
                init_cols.forEach(function(col) {
                  mem_obj.columns.push(that.new_object(key, super_column_name, col._name, col));
                })
              } else {
                init_cols.forEach(function(col) {                
                  mem_obj.columns.push(col);
                })
              }
            } else {
              throw "Cannot use an array to initialize this object."              
            }
          }
        }  
        return mem_obj;
      }
      cf.find = func_for_column_family(ks.name, cf.name, find_objects)
      cf.remove = function(key, event_listeners) {
        remove_object(ks.name, this.name, key, null, null, event_listeners)
      }
    }
    var request = ks.cassandra.create_request("describe_keyspace", {keyspace: ks.name});
    request.addListener("success", function(result) {
      for (var cf_name in result) {
        var cf = keyspaces[ks.name].column_families[cf_name];
        if (!cf) keyspaces[ks.name].column_families[cf_name] = cf = {};
        cf.type = result[cf_name].Type;
      }
      logger.info("Initialized column family types with Cassandra keyspace description for " + ks.name);
    });
    request.addListener("error", function(mess) {
      logger.error("Failed to get keyspace description from Cassandra for " + 
                      ks.name + ":" + mess);
    });
    request.send();
  })
}

exports.set_logger = function(a_logger) {
  logger = a_logger;
}

exports.get_column_family = get_column_family = function(keyspace, column_family) {
  return keyspaces[keyspace].column_families[column_family];
}

exports.ConsistencyLevel = ConsistencyLevel = {
  ZERO: 0, ONE: 1, QUORUM: 2, DCQUORUM:3, DCQUORUMSYNC: 4  
}

exports.low_level = {};
[ 
  create_mem_object, mutations_for_save_row_object, 
  mutations_for_save_super_column_object, mutations_for_save_column_object,
  mutations_for_destroy_row_object, 
  mutations_for_destroy_super_column_object, mutation_for_destroy_column_object
].forEach(function(f) {
  exports.low_level[f.name] = f;
})