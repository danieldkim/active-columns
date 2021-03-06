var sys = require('sys');
var _ = require('underscore')._
var events = require('events')
var async = require('async')

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
  
  var keyspace, column_family, key_spec, super_column_name, column_name_or_predicate, find_callback;
  var key, keys, range, column_name, predicate;
  keyspace = arguments[0];
  column_family = arguments[1];
  var cf = get_column_family(keyspace, column_family)
  find_callback = arguments[arguments.length-1];
  var penultimate_arg = arguments[arguments.length-2];
  if (penultimate_arg.constructor.name == 'Object' && arguments.length > 4) {
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
      throw Error("Must specify a column name or predicate.");
    }
  } else if (column_name_or_predicate.constructor.name == 'Object') {
    predicate = column_name_or_predicate;
  } else {
    column_name = column_name_or_predicate;
    predicate = {column_names:[column_name]};
  }

  // logger.debug("find_objects - keyspace: " + keyspace + ",column_family:" + column_family + 
  //              ",key:" + key + ",keys:" + sys.inspect(keys, false, null) + ",range:" + sys.inspect(range, false, null) +  
  //              ",super_column_name:" + super_column_name + ",column_name:" + column_name + 
  //              ",predicate:" + sys.inspect(predicate, false, null));
  var cassandra = keyspaces[keyspace].cassandra,
      column_parent = {column_family: column_family},
      get_request;
  if (super_column_name) column_parent.super_column = super_column_name;
  if (key) {
    get_request = function(callback) {
      cassandra.get_slice(keyspace, key, column_parent, predicate, 
        ConsistencyLevel.ONE, callback);
     };
  } else if (range) {
    get_request = function(callback) {
      cassandra.get_range_slices(keyspace, column_parent, predicate, range, 
        ConsistencyLevel.ONE, callback);
    };
  } else { // must be keys
    get_request = function(callback) {
      cassandra.multiget_slice(keyspace, keys, column_parent, predicate,
        ConsistencyLevel.ONE, callback);
    };
  }
  
  var cf = get_column_family(keyspace, column_family);
  var after_find_callbacks, init_callbacks;
  if (column_name) {
    after_find_callbacks = cf.callbacks.after_find_column || [];
    init_callbacks = cf.callbacks.after_initialize_column || [];
  } else if (super_column_name) {
    after_find_callbacks = cf.callbacks.after_find_super_column || [];
    init_callbacks = cf.callbacks.after_initialize_super_column || [];
  } else {
    after_find_callbacks = cf.callbacks.after_find_row || [];
    init_callbacks = cf.callbacks.after_initialize_row || [];
  }
  
  get_request(function(err, result) {
    if (err) {
      var error_mess = "Error finding object(s) in " + keyspace + "." + 
                       column_family + ": " + err;
      logger.error(error_mess);
      find_callback(new Error(error_mess));
      return;
    }
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
        call_callbacks_sequentially(after_find_callbacks, object_result, function() {
          init_callbacks.forEach(function(cb) {
            cb.call(object_result);
          });
          object_result_ready();          
        }, function(err) {
          find_callback(err);
        })
      } else {
        object_result_ready();
      }                                              
    } else if (range) { // list of keyslices 
      object_result = [];
      var callback_counter = 0;
      var non_empty_results = [];
      result.forEach(function(ks){
        var columns, timestamp;
        if (column_name) {
          columns = ks.columns[0].value
          timestamp = ks.columns[0].timestamp
        } else {
          columns = ks.columns;
        }
        if (columns.length > 0) {
          non_empty_results.push({key: ks.key, columns:columns, timestamp: timestamp})
          callback_counter++;
        }
      });
      if (non_empty_results.length < 1) object_result_ready();
      non_empty_results.forEach(function(res){
        var o = create_mem_object(keyspace, column_family, res.key, 
          super_column_name, column_name, res.columns, res.timestamp)
        o.update_last_saved();
        object_result.push(o);
        call_callbacks_sequentially(after_find_callbacks, o, function() {
          init_callbacks.forEach(function(cb) {
            cb.call(o);
          });
          callback_counter--;
          if (callback_counter == 0) object_result_ready();
        }, function(err) {
          find_callback(err);
        })
      })
    } else { // hash of lists of columns or super columns
      object_result = {}
      var callback_counter = 0;
      var non_empty_results = {};
      _.forEach(result, function(columns, k) {
        if (column_name) {
          columns = columns[0].value
          timestamp = columns[0].timestamp
        }
        if (columns.length > 0) {
          callback_counter++;
          non_empty_results[k] = {columns:columns, timestamp:timestamp};
        }
      });
      if (Object.keys(non_empty_results).length < 1) object_result_ready();
      _.forEach(non_empty_results, function(res, k) {
        object_result[k] = create_mem_object(keyspace, column_family, k, 
                             super_column_name, column_name, res.columns, res.timestamp)
        object_result[k].update_last_saved();
        call_callbacks_sequentially(after_find_callbacks, object_result[k], function() {
          init_callbacks.forEach(function(cb) {
            cb.call(object_result[k]);
          });
          callback_counter--;
          if (callback_counter == 0) object_result_ready();
        }, function(err) {
          find_callback(err);
        })
      });
    }
    
    function object_result_ready() {
      if (object_result) {
        if (logger.isDebugEnabled()) {
          logger.debug("Found object(s) in " + keyspace + "." + column_family + 
                       ": " + sys.inspect(object_result, false, null));
        }
        find_callback(null, object_result);
      } else {
        find_callback(null, null);
      }      
    }
  });
}

function remove_object(keyspace, column_family, key, super_column_name, column_name, callback) {
  var cassandra = keyspaces[keyspace].cassandra;
  var cf = get_column_family(keyspace, column_family)
  var column_path = {column_family: column_family}
  if (super_column_name) column_path.super_column = super_column_name;
  if (column_name) column_path.column = column_name;
  cassandra.remove(keyspace, column_path, "auto", 
    ConsistencyLevel.ONE, function(err, result) {

    if (err) {
      var error_mess = "Error trying to remove " + 
        path_s(keyspace, column_family, key, super_column_name, column_name) + 
        ":" + err;
      if (callback) callback(new Error(error_mess));
      return;      
    } else  {
      if (callback) callback(null, result);
    }
    
  });
}

function save_row_object(keyspace, column_family, o, callback, delete_missing_columns) {
  if (typeof delete_missing_columns == 'undefined') {
    delete_missing_columns = true
  }      
  callback = insert_after_callbacks(keyspace, column_family, callback, 
                         "after_save_row", o);
  save_object(keyspace, column_family, o.key, o, true, callback, 
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
        if (o[name]) 
          memo.push({
            name:name, 
            value: pre_serialize_column_value(cf, name, o[name]), 
            timestamp: "auto"
          }); 
        return memo;
      })
    } else {
      mutations = _.map(o.columns, function(col) { 
        return {
          name:col.name, 
          value: pre_serialize_column_value(cf, col.name, col.value), 
          timestamp: "auto"
        };
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

function save_super_column_object(keyspace, column_family, key, o, callback, delete_missing_columns) {
  callback = insert_after_callbacks(keyspace, column_family, callback, 
                         "after_save_super_column", o);
  save_object(keyspace, column_family, key, o, true, callback, 
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
        if (o[name]) 
          memo.push({
            name:name, 
            value: pre_serialize_column_value(cf, name, o[name]), 
            timestamp: "auto"
          });
        return memo;
      })
    } else {
      insert_columns = _.map(o.columns, function(col) { 
        return {
          name:col.name, 
          value: pre_serialize_column_value(cf, col.name, col.value), 
          timestamp: "auto"
        };
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

function save_column_object(keyspace, column_family, key, super_column_name, o, callback) {
  callback = insert_after_callbacks(keyspace, column_family, callback, 
                         "after_save_column", o);
  save_object(keyspace, column_family, key, o, true, callback, 
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

function pre_serialize_column_value(column_family, name, value) {
  if (column_family.column_value_types &&
      column_family.column_value_types[name] == 'json')
    return JSON.stringify(value);
  else
    return value;
}

function destroy_row_object(keyspace, column_family, o, callback) {
  callback = insert_after_callbacks(keyspace, column_family, callback, 
                         "after_destroy_row", o);
  save_object(keyspace, column_family, o.key, o, false, callback, 
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

function destroy_super_column_object(keyspace, column_family, key, o, callback) {
  callback = insert_after_callbacks(keyspace, column_family, callback, 
                         "after_destroy_super_column", o);
  save_object(keyspace, column_family, key, o, false, callback, 
    function() {
      return mutations_for_destroy_super_column_object(keyspace, column_family, o);
    },
    function(new_timestamp) {
      if (o.timestamps) {
        _.forEach(o.timestamps, function(v, k) {
          o.timestamps[k] = new_timestamp;
        });
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

function destroy_column_object(keyspace, column_family, key, super_column_name, o, callback) {
  callback = insert_after_callbacks(keyspace, column_family, callback, 
                         "after_destroy_column", o);
  save_object(keyspace, column_family, key, o, false, callback, 
    function() { return [mutation_for_destroy_column_object(super_column_name, o)]; })  
}

function mutation_for_destroy_column_object(super_column_name, o) {
  if (!o.timestamp) throw Error("Cannot destroy a column object without a timestamp!");
  var mut = {timestamp: o.timestamp, predicate: {column_names: [o._name]}}
  if (super_column_name) mut.super_column = super_column_name;
  return mut;
}

function save_object(keyspace, column_family, key, o, auto_generate_ids, save_callback, mutations_func, timestamp_func, update_last_saved) {
  // logger.debug("save_object - keyspace: " + keyspace + ",column_family:" + column_family + 
  //          ",key:" + key + ",o:" + sys.inspect(o, false, null));
           
  var cassandra = keyspaces[keyspace].cassandra;

  if (update_last_saved && o.before_save_callbacks &&
      o.before_save_callbacks.length > 0 ) {
    var previous_version = o._last_saved;
    call_callbacks_sequentially(o.before_save_callbacks, o, function() {
      check_key();
    }, function(err) {
      if (save_callback) save_callback(err);        
    },
    true, previous_version);
  } else {
    check_key();
  }

  function check_key() {
    if ( key ) {
      now_have_key();
    } else if (o.key) {
      key = o.key;
      now_have_key();
    } else if (!auto_generate_ids) {
      throw Error("Cannot save/destroy an object without a key.");
    } else {
      cassandra.get_uuids(function(err, result) {
        if (err) {
          var error_mess = "Could not get UUID for key when attempting to save object under " + 
            path_s(keyspace, column_family, key) + ':'  + err;
          logger.error(error_mess);
          if (save_callback) save_callback(new Error(error_mess));
          return;
        }
        o.key = key = result[0];
        now_have_key();
      })
    }    
  }
  
  function now_have_key() {
    if ( o.id ) {
      do_save();
    } else if (!auto_generate_ids) {
      throw Error("Cannot save/destroy an object without a _name.");
    } else {
      cassandra.get_uuids(function(err, result) {
        if (err) {
          var error_mess = "Could not get UUID for id when attempting to save/destroy object under " +
                            path_s(keyspace, column_family, key) + ': ' + err;
          logger.error(error_mess);
          if (save_callback) save_callback(new Error(error_mess));
          return;          
        }
        o._name = result[0];
        do_save();
      })
    }     
  }
      
  function do_save() {
    var mut_map = {};
    mut_map[key] = {};
    var mutations = mut_map[key][column_family] = mutations_func();
    if (!mutations || mutations.length < 1) throw Error("Nothing to save/destroy!");
    cassandra.batch_mutate(keyspace, mut_map,
      ConsistencyLevel.ONE, function(err, result) {
      if (err) {
        var error_mess = "Error saving/destroying object under '" + 
                          path_s(keyspace, column_family, key) + ": " + err;
        logger.error(error_mess);
        if (save_callback) save_callback(new Error(error_mess));
        return;
      }
      if (timestamp_func) timestamp_func(result);
      if (update_last_saved) o.update_last_saved();
      if (save_callback) save_callback(null, o.id);
    });
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
  var do_activate = true;
  if (column_name) { // value under a column name
    var subcolumn_value_type = cf.subcolumn_value_type
    var column_value_type = cf.column_value_type
    var column_value_type_for_column;
    if (cf.column_value_types) 
      column_value_type_for_column = cf.column_value_types[column_name];
    if ((super_column_name && subcolumn_value_type && subcolumn_value_type  == 'json')
         ||
         (!super_column_name && column_value_type && column_value_type == 'json')) {
      if ( logger.isDebugEnabled() )
        logger.debug("Creating json value " + sys.inspect(columns, false, null) + " under " + path);
      o = eval('(' + columns + ')')
      if (timestamp) o.timestamp = timestamp;
    } else if (column_value_type_for_column == 'json') {
      if ( logger.isDebugEnabled() )
        logger.debug("Creating json value " + sys.inspect(columns, false, null) + " under " + path);
      o = eval('(' + columns + ')');
      do_activate = false;
    } else if (column_value_type_for_column == 'date') {
      o = new Date(columns);
      if ( logger.isDebugEnabled() )
        logger.debug("Returning date " + o + " under " + path);
      do_activate = false;
    } else if (column_value_type_for_column == 'number') {
      o = +columns;
      if ( logger.isDebugEnabled() )
        logger.debug("Returning number " + o + " under " + path);
      do_activate = false;
    } else {
      if ( logger.isDebugEnabled() )
        logger.debug("Returning value " + sys.inspect(columns, false, null) + " under " + path);
      o = columns;
      do_activate = false;
    }      
  } else if ( super_column_name ) { // object under a super column
    if ( cf.subcolumn_names ) {
      if ( logger.isDebugEnabled() ) {
        logger.debug("Creating super column object with subcolumn_names " + 
                     sys.inspect(cf.subcolumn_names, false, null) + " under " + path);
      }
      Object.defineProperty(o, "timestamps", {value: {}})
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
        if (value.constructor.name == 'Object') {
          o.columns.push(value);
        } else {
          o.columns.push({
            name: col.name, 
            value: pre_serialize_column_value(cf, col.name, value)
          });          
        }
      })
    }
  } else  { // object under a key
    if (cf.type == "Super") { 
      if ( cf.column_names ) {
        if ( logger.isDebugEnabled() ) {
          logger.debug("Creating row object with super column names " +
            sys.inspect(cf.column_names, false, null) + " under " + path);
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
        Object.defineProperty(o, "timestamps", {value: {}})
        if ( logger.isDebugEnabled() ) {
          logger.debug("Creating row object with column names " +
            sys.inspect(cf.column_names, false, null) + " under " + path);
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
          if (value.constructor.name == 'Object') {
            o.columns.push(value);
          } else {
            o.columns.push({
              name: col.name, 
              value: pre_serialize_column_value(cf, col.name, value), 
              timestamp: col.timestamp
            });
          }                    
        })
      }
    }
  }
  
  if (do_activate) 
    activate_object(keyspace, column_family, key, super_column_name, column_name, o);

  return o;
}

function activate_object(keyspace, column_family, key, super_column_name, column_name, o) {
  // logger.info("--- activate_object path: " + path_s(keyspace, column_family, key, super_column_name, column_name))  
  
  function row_key() { return key; }
  function this_name() { return this._name; }
  function set_this_name(val) { this._name = val; }
  Object.defineProperty(o, "_last_saved", {value: null, writable: true});
  o.update_last_saved = function() {

    function copy(thing) {
      var a_copy;
      if (thing.constructor.name == "Object") {
        a_copy = {}
        for (var k in thing) {
          a_copy[k] = copy(thing[k]);
        }
      } else if (thing.constructor.name == 'Date') {
        a_copy = new Date(thing);
      } else {
        a_copy = thing;
      }
      return a_copy;
    }
    
    this._last_saved = {};
    for (var k in this) {
      if (["_last_saved", "update_last_saved"].indexOf(k) > -1) 
        continue;
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
    o._name = column_name;
    Object.defineProperty(o, "id", { get: this_name, set: set_this_name, enumerable: true });
    Object.defineProperty(o, "key", { get: row_key, enumerable: true });
    Object.defineProperty(o, "after_initialize_callbacks", { 
      get: function() { return cf.callbacks.after_initialize_column;} 
    });
    Object.defineProperty(o, "before_save_callbacks", { 
      get: function() { return cf.callbacks.before_save_column;} 
    });
    o.get_super_column_name = function() { return super_column_name};
    o.save = function(callback, delete_missing_columns) {
      save_column_object(keyspace, column_family, key, super_column_name, this, callback);
    }
    o.destroy = function(callback) {
      destroy_column_object(keyspace, column_family, key, super_column_name, this, callback);
    }
  } else if (super_column_name) {
    o._name = super_column_name;
    Object.defineProperty(o, "id", { get: this_name, set: set_this_name, enumerable: true });
    Object.defineProperty(o, "key", { get: row_key, enumerable: true });
    Object.defineProperty(o, "after_initialize_callbacks", { 
      get: function() { return cf.callbacks.after_initialize_super_column;} 
    });
    Object.defineProperty(o, "before_save_callbacks", { 
      get: function() { return cf.callbacks.before_save_super_column;} 
    });
    if (!o.columns && !cf.subcolumn_names) o.columns = [];
    if (!o.timestamps && cf.subcolumn_names) 
      Object.defineProperty(o, "timestamps", {value: {}});
    o.save = function(callback, delete_missing_columns) {
      save_super_column_object(keyspace, column_family, key, this, 
        callback, delete_missing_columns);
    }  
    o.destroy = function(callback) {
      destroy_super_column_object(keyspace, column_family, key, this, callback);
    }
  } else {
    o.key = key;
    if (!o.columns && !cf.column_names) o.columns = [];      
    if (!o.timestamps && cf.column_names) 
      Object.defineProperty(o, "timestamps", {value: {}});
    Object.defineProperty(o, "id", { 
      get: function() { return this.key; },
      set: function(val) { this.key = val; }, 
      enumerable: true
    });
    Object.defineProperty(o, "after_initialize_callbacks", { 
      get: function() { return cf.callbacks.after_initialize_row;} 
    });
    Object.defineProperty(o, "before_save_callbacks", { 
      get: function() { return cf.callbacks.before_save_row;} 
    });
    o.save = function(callback, delete_missing_columns) {
      save_row_object(keyspace, column_family, this, callback, delete_missing_columns);
    }
    o.destroy = function(callback, totally) {
      if (totally) {
        get_column_family(keyspace, column_family).remove(this.key, callback);
      } else {
        destroy_row_object(keyspace, column_family, this, callback);        
      }
    }    
  }
  return o;
}

function insert_after_callbacks(keyspace, column_family, original_callback, callback_names, o) {
  if (!Array.isArray(callback_names)) callback_names = [callback_names];
  var cf = get_column_family(keyspace, column_family)
  var callbacks = []
  callback_names.forEach(function(cb_name) {
    var cb_name_callbacks = cf.callbacks[cb_name];
    if (cb_name_callbacks && cb_name_callbacks.length > 0) 
      callbacks = callbacks.concat(cb_name_callbacks);
  })
  if (callbacks.length < 1) return original_callback;
  var previous_version = o._last_saved;
  var new_callback = function(err, result) {
    if (err) {
      original_callback(err, result);
      return;
    }
    call_callbacks_sequentially(callbacks, o, function() {
      original_callback(err, result);
    }, function(err) {
      callback(err);        
    }, 
    true, previous_version);    
  }
  return new_callback;
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

function call_callbacks_sequentially(callbacks, o, finish, error_handler, with_previous_version, previous_version, i) {
  if (callbacks.length < 1) {
    finish();
    return;
  }
  i = i || 0;
  var cb = callbacks[i];
  args = [];
  if (with_previous_version) args.push(previous_version);
  args.push(function(err) {
    if (err) {
      error_handler(err);
    } else {
      if (i < callbacks.length - 1) 
        call_callbacks_sequentially(callbacks, o, finish, error_handler, with_previous_version, previous_version, i+1);
      else finish();
    }
  });
  cb.apply(o, args);
}

function initialize_keyspaces(config, callback) {
  var steps = [];
  _.forEach(config, function(keyspace_config, keyspace_name){
    steps.push(function(callback) {
      initialize_keyspace(keyspace_name, keyspace_config, callback);      
    });
  });
  async.parallel(steps, function(err) {
    if (err) callback(err);
    else callback(null, keyspaces);
  })
};

function initialize_keyspace(keyspace_name, config, callback) {

  var ks = keyspaces[keyspace_name] = {}
  ks.name = keyspace_name
  ks.column_families = {}
  if (config.column_families) {
    _.forEach(config.column_families, function(cf_config, cf_name) {
      initialize_column_family(ks.name, cf_name, cf_config);
    });
  }    
  ks.cassandra = require('cassandra-node-client').create(
                   config.cassandra_port, config.cassandra_host, logger)
  ks.cassandra.describe_keyspace(ks.name, function(err, result) {
    if (err) {
     logger.error("Failed to get keyspace description from Cassandra for " + 
                     ks.name + ":" + err);
     callback(err); 
    }
    _.forEach(result, function(column_family, column_family_name) {
      ks.column_families[column_family_name].type = column_family.Type;
    });
    callback(null, ks);
  });  
}

function initialize_column_family(keyspace_name, column_family_name, config) {
  var ks = keyspaces[keyspace_name];
  if (!ks) throw Error("No keyspace with name " + keyspace_name);
  var cf = ks.column_families[column_family_name] = {
    name: column_family_name,
    type: config.type,
    column_names: config.column_names,
    column_value_type: config.column_value_type,
    column_value_types: config.column_value_types,
    subcolumn_names: config.subcolumn_names,
    subcolumn_value_type: config.subcolumn_value_type,
    callbacks: config.callbacks || {}
  };
  
  cf.add_callback = function(name, func) {
    var cb_list = this.callbacks[name];
    if (!cb_list) cb_list = this.callbacks[name] = [];
    cb_list.push(func);         
  }
  cf.new_object = function() {
    // logger.info("new_object - arguments: " + sys.inspect(arguments, false, null));

    var key, super_column_name, column_name, init_cols;
    var last_arg = arguments[arguments.length-1];
    if (last_arg && ['Object', 'Array'].indexOf(last_arg.constructor.name) > -1) {
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
    // logger.info("new_object - keyspace: " + keyspace_name + ",column_family:" + column_family_name + 
    //          ",key:" + key + ",super_column_name:" + super_column_name + ",column_name:" + column_name +
    //          ",init_cols:" + sys.inspect(init_cols, false, null) + ",columns:" + sys.inspect(columns, false, null));
    var mem_obj = create_mem_object(keyspace_name, column_family_name, key, super_column_name,
                      column_name, columns);
                      
    if (init_cols) { 
      if (init_cols.constructor.name == 'Object') {                                                
        // can initialize with a hash if this is:
        // - a column object, or
        // - a row object with fixed column names or json column value type, or
        // - a super column object with fixed subcolumn names or json column value type
        if (column_name || 
            (!super_column_name && 
              (this.column_names || this.column_value_type == 'json')) ||
            (super_column_name && 
              (this.subcolumn_names || this.subcolumn_value_type == 'json'))) {
          var that = this;
          _.forEach(init_cols, function(val, name) {
            if (val.constructor.name == 'Object') {
              if ( !super_column_name && 
                  (that.column_names || that.column_value_type == 'json') ) {
                mem_obj[name] = that.new_object(key, name, val);
              } else if ( super_column_name && 
                          (that.subcolumn_names || that.subcolumn_value_type == 'json') ) {
                mem_obj[name] = that.new_object(key, super_column_name, name, val);
              } else {
                throw Error("Cannot use a hash to initialize this object.");
              }
            } else {
              mem_obj[name] = val;
            }
          });
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
          throw Error("Cannot use an array to initialize this object.");
        }
      }
    }
    
    if (column_name) {
      init_callbacks = this.callbacks.after_initialize_column || [];
    } else if (super_column_name) {
      init_callbacks = this.callbacks.after_initialize_super_column || [];
    } else {
      init_callbacks = this.callbacks.after_initialize_row || [];
    }
    init_callbacks.forEach(function(cb) {
      cb.call(mem_obj);
    })        
    return mem_obj;
  }
  cf.find = func_for_column_family(ks.name, cf.name, find_objects)
  cf.remove = function(key, callback) {
    remove_object(ks.name, this.name, key, null, null, callback)
  }
  
  return cf;
}

exports.initialize_keyspaces = initialize_keyspaces;
exports.initialize_keyspace = initialize_keyspace;
exports.initialize_column_family = initialize_column_family;
exports.keyspaces = keyspaces;

exports.set_logger = function(a_logger) {
  logger = a_logger;
}

exports.get_column_family = get_column_family = function(keyspace_name, column_family) {
  return keyspaces[keyspace_name].column_families[column_family];
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