var sys = require('sys');
var _ = require('underscore')._;
var assert = require('assert');

var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
var logger = log4js.getLogger(logger_name);
logger.setLevel('INFO');

var test_helpers = require('./test-helpers') 
test_helpers.set_logger(logger);

var ActiveColumns = require('active-columns');
ActiveColumns.set_logger(logger);

var tests = {
  "No supercolumn, no column names": function() {
    _test_create_mem_object(null, false)  
  },
  
  "No supercolumn, no column names, json value type": function() {
    _test_create_mem_object(null, false, 'json')  
  },
  
  "No supercolumn, fixed column names": function() {
    _test_create_mem_object(null, true)  
  },
  
  "No supercolumn, fixed column names, json value type": function() {
    _test_create_mem_object(null, true, 'json')
  },
  
  "Supercolumn, no column names": function() {
    _test_create_mem_object("test_super_column", false)  
  },
  
  "Supercolumn, no column names, json value type": function() {
    _test_create_mem_object("test_super_column", false, 'json')  
  },
  
  "Supercolumn, fixed column names": function() {
    _test_create_mem_object("test_super_column", true)  
  },

  "Supercolumn, fixed column names, json value type": function() {
    _test_create_mem_object("test_super_column", true, 'json')
  }
}

for (var test_name in tests) {
  test_helpers.run_sync_test(test_name, tests[test_name]);
}

function _test_create_mem_object(name, fixed_names, column_value_type) {
  var cf = { name: "TestColumnFamily"}
  if (name) cf.column_type = "Super"
  cf[(name ? 'sub' : '') + 'column_names'] = fixed_names ? ["col_1", "col_2"] : undefined,
  cf[(name ? 'sub' : '') + 'column_value_type'] = column_value_type
  
  ActiveColumns.initialize_keyspaces([ {name: "TestKeyspace", column_families: [cf]} ]);

  var val_1 = '{"col_a":"1/a", "col_b":"1/b"}',
      val_2 = '{"col_a":"2/a", "col_b":"2/b"}'
  var o = ActiveColumns.low_level.create_mem_object("TestKeyspace", "TestColumnFamily", "test_key", 
            name, null, 
            [{name:"col_1", value: val_1, timestamp: 1},
             {name:"col_2", value: val_2, timestamp: 2}])
// sys.puts(" --- cf: " + JSON.stringify(cf))  
// sys.puts(" --- o: " + JSON.stringify(o))  
  if (name) {
   assert.equal(name, o.name)
  } else {
   assert.equal("test_key", o.key)
  }
  if (fixed_names) {
    if (column_value_type && column_value_type == 'json') {
      assert.equal(1, o.timestamps.col_1);
      assert.equal(2, o.timestamps.col_2);
      assert.equal("col_1", o.col_1.name);
      assert.equal("1/a", o.col_1.col_a);
      assert.equal("1/b", o.col_1.col_b);
      assert.equal("col_2", o.col_2.name);
      assert.equal("2/a", o.col_2.col_a);
      assert.equal("2/b", o.col_2.col_b);       
    } else {
      assert.equal(1, o.timestamps.col_1);
      assert.equal(2, o.timestamps.col_2);
      assert.equal(val_1, o.col_1);
      assert.equal(val_2, o.col_2);       
    }
  } else {
    if (column_value_type && column_value_type == 'json') {
      assert.equal("col_1", o.columns[0].name);
      assert.equal("col_1", o.columns[0].name);
      assert.equal("1/a", o.columns[0].col_a);
      assert.equal("1/b", o.columns[0].col_b);
      assert.equal(1, o.columns[0].timestamp);
      assert.equal("col_2", o.columns[1].name);
      assert.equal("col_2", o.columns[1].name);
      assert.equal("2/a", o.columns[1].col_a);
      assert.equal("2/b", o.columns[1].col_b);                   
      assert.equal(2, o.columns[1].timestamp);
    } else {
      assert.equal(val_1, o.columns[0].value);
      assert.equal(val_2, o.columns[1].value);
    }
  }

}

