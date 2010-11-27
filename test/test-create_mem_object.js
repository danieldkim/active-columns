require.paths.unshift('./lib');
require.paths.unshift('../lib');

var sys = require('sys');
var _ = require('underscore')._;
var assert = require('assert');

var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
var logger = log4js.getLogger(logger_name);
logger.setLevel('INFO');

var ActiveColumns = require('active-columns');
ActiveColumns.set_logger(logger);

var test_util = require('test-util');
var async_testing = require('async_testing')
  , wrap = async_testing.wrap
  ;

// if this module is the script being run, then run the tests:  
if (module == require.main) {
  test_util.run(__filename, module.exports);
}

var suite = wrap({
  suiteSetup: function(done) {
    done();
  },
  setup: function(test, done) {
    done();
  },
  teardown: function(test, done) {
    done();
  },
  suite: {
    "No supercolumn, no column names": function(assert) {
      _test_create_mem_object(assert, null, false)  
    },

    "No supercolumn, no column names, json value type": function(assert) {
      _test_create_mem_object(assert, null, false, 'json')  
    },

    "No supercolumn, fixed column names": function(assert) {
      _test_create_mem_object(assert, null, true)  
    },

    "No supercolumn, fixed column names, json value type": function(assert) {
      _test_create_mem_object(assert, null, true, 'json')
    },

    "Supercolumn, no column names": function(assert) {
      _test_create_mem_object(assert, "test_super_column", false)  
    },

    "Supercolumn, no column names, json value type": function(assert) {
      _test_create_mem_object(assert, "test_super_column", false, 'json')  
    },

    "Supercolumn, fixed column names": function(assert) {
      _test_create_mem_object(assert, "test_super_column", true)  
    },

    "Supercolumn, fixed column names, json value type": function(assert) {
      _test_create_mem_object(assert, "test_super_column", true, 'json')
    }
  },
  suiteTeardown: function(done) {
    done();
  }  
});

module.exports = { "create_mem_object() tests": suite };

function _test_create_mem_object(assert, name, fixed_names, column_value_type) {
  var cf = {}
  if (name) cf.column_type = "Super"
  cf[(name ? 'sub' : '') + 'column_names'] = fixed_names ? ["col_1", "col_2"] : undefined,
  cf[(name ? 'sub' : '') + 'column_value_type'] = column_value_type
  
  ActiveColumns.initialize_keyspaces({ "TestKeyspace": { column_families: {"TestColumnFamily":cf} } });

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

