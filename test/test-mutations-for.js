var assert = require('assert');
var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
var logger = log4js.getLogger(logger_name);
logger.setLevel('INFO');
var sys = require('sys')
var _ = require('underscore')._

var ActiveColumns = require('active-columns');
ActiveColumns.set_logger(logger);
require('./init-test-keyspace').do_it();

var TestSuite = require('async_testing').TestSuite;
var suite = new TestSuite("Mutations tests");
suite.addTests({
  "Mutations for Users1": function (assert) {
    var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");
    var alice = Users1.new_object("alice", {
      city: "New York", state: "NY", last_login: 1271184168, sex: "F"
    });
    var mutations = ActiveColumns.low_level.mutations_for_save_row_object(
      "ActiveColumnsTest", "Users1", alice)
    assert.equal(4, mutations.length);
    [ 
      { name: 'city', value: 'New York', timestamp: 'auto' }, 
      { name: 'state', value: 'NY', timestamp: 'auto' }, 
      { name: 'last_login', value: 1271184168 , timestamp: 'auto'},
      { name: 'sex', value: 'F' , timestamp: 'auto'}
    ].forEach(function(exp_mut) {
      assert.ok(_.any(mutations, function(mut) {
        return exp_mut.name == mut.name && exp_mut.value == mut.value && exp_mut.timestamp == mut.timestamp;
      }), "Mutations did not contain expected mutation: " + sys.inspect(exp_mut, false, null));     
    })
    
    
    alice.timestamps.city = alice.timestamps.last_login = 1270586369995573;
    
    mutations = ActiveColumns.low_level.mutations_for_destroy_row_object(
      "ActiveColumnsTest", "Users1", alice)
    // sys.puts(sys.inspect(mutations, false, null))
    
    alice.timestamps.last_login = 1270586369995574;  
    mutations = ActiveColumns.low_level.mutations_for_destroy_row_object(
      "ActiveColumnsTest", "Users1", alice)
    // sys.puts(sys.inspect(mutations, false, null))
    
    delete alice.city; 
    delete alice.last_login;
    delete alice.sex; // this one should not appear as delete mutation since it has no timestamp
    mutations = ActiveColumns.low_level.mutations_for_save_row_object(
      "ActiveColumnsTest", "Users1", alice, true);
    assert.equal(3, mutations.length);
    var exp_insert = { name: 'state', value: 'NY', timestamp: 'auto' };
    assert.ok(_.any(mutations, function(mut) {
      return exp_insert.name == mut.name && exp_insert.value == mut.value && 
             exp_insert.timestamp == mut.timestamp;
    }), "Mutations did not contain expected mutation: " + sys.inspect(exp_insert, false, null));     
    [
      {"timestamp":1270586369995573, "predicate": {"column_names":["city"]}},
      {"timestamp":1270586369995574,"predicate":{"column_names":["last_login"]}}
    ].forEach(function(exp_del) {
     assert.ok(_.any(mutations, function(mut) {
       return exp_del.timestamp == mut.timestamp && 
              mut.predicate.column_names.length == 1 && 
              exp_del.predicate.column_names[0] == mut.predicate.column_names[0];
     }), "Mutations did not contain expected deletion: " + sys.inspect(exp_del, false, null))
    })
        
  },
  
  "Mutations for Users2": function (assert) {
    var Users2 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users2");
    var alice = Users2.new_object("alice", [
      {name: "city", value: "New York"},
      {name: "state", value: "NY"},
      {name: "last_login", value: 1271184168}
    ])
    // var alice = Users2.new_object("alice");
    // alice.add_column("city", "New York");
    // alice.add_column("state", "NY");
    // alice.add_column("last_login", 1271184168);
    var mutations = ActiveColumns.low_level.mutations_for_save_row_object(
      "ActiveColumnsTest", "Users2", alice)
    assert.equal(3, mutations.length);
    [ 
      { name: 'city', value: 'New York', timestamp: 'auto' }, 
      { name: 'state', value: 'NY', timestamp: 'auto' }, 
      { name: 'last_login', value: 1271184168 , timestamp: 'auto'}
    ].forEach(function(exp_mut) {
      assert.ok(_.any(mutations, function(mut) {
        return exp_mut.name == mut.name && exp_mut.value == mut.value && exp_mut.timestamp == mut.timestamp;
      }), "Mutations did not contain expected mutation: " + sys.inspect(exp_mut));     
    })    
  },
  
  "Mutations for StateUsers1": function (assert) {
    var StateUsers1 = ActiveColumns.get_column_family("ActiveColumnsTest", "StateUsers1");
    var ny = StateUsers1.new_object("ny", [
      {_name: "alice", sex: "F", city: "New York"},
      {_name: "bob", sex: "M", city: "Jackson Heights"}
    ])
    // var ny = StateUsers1.new_object("ny");
    // ny.add_column("alice", {sex: "F", city: "New York"});
    // ny.add_column("bob", {sex: "M", city: "Jackson Heights"});
    var mutations = ActiveColumns.low_level.mutations_for_save_row_object("ActiveColumnsTest", "StateUsers1", ny);
    assert.equal(2, mutations.length);
    var expected_mutations = [
      { "name":"alice",
        "columns":[
          {"name":"city","value":"New York","timestamp":"auto"},
          {"name":"sex","value":"F","timestamp":"auto"}
        ]
      },
      {"name":"bob",
       "columns":[
          {"name":"city","value":"Jackson Heights","timestamp":"auto"},
          {"name":"sex","value":"M","timestamp":"auto"}
       ]
      }
    ]
    expected_mutations.forEach(function(exp_mut) {
      assert.ok(_.any(mutations, function(mut) {
        if (exp_mut.name != mut.name) return false;
        exp_mut.columns.forEach(function(exp_col) {
          assert.ok(_.any(mut.columns, function(col) {
            return exp_col.name == col.name && exp_col.value == col.value && exp_col.timestamp == col.timestamp;
          }), 
          "Mutations for super column " + mut.super_column + 
          " did not contain expected column: " + sys.inspect(exp_col));
        })
        return true;
      }), "Mutations did not contain expected mutation: " + sys.inspect(exp_mut));     
    })
    mutations = ActiveColumns.low_level.mutations_for_save_super_column_object(
      "ActiveColumnsTest", "StateUsers1", ny.columns[0]);
    assert.equal(1, mutations.length);
    var exp_mut = expected_mutations[0], mut = mutations[0];
    assert.equal(exp_mut.super_column, mut.super_column, 
                 "Mutation did not contain super column " + exp_mut.super_column);
    exp_mut.columns.forEach(function(exp_col) {
      assert.ok(_.any(mut.columns, function(col) {
        return exp_col.name == col.name && exp_col.value == col.value && exp_col.timestamp == col.timestamp;
      }), 
      "Mutations for super column " + mut.super_column + 
      " did not contain expected column: " + sys.inspect(exp_col));
    })
  },
   
  "Mutations for StateUsers2": function (assert) {
    var StateUsers2 = ActiveColumns.get_column_family("ActiveColumnsTest", "StateUsers2");
    var ny = StateUsers2.new_object("NY", [
      {_name: "alice", state: "NY", city: "New York"},
      {_name: "bob", state: "NY", city: "Jackson Heights"}
    ])
    // var ny = StateUsers2.new_object("NY")
    // ny.add_column("alice", {state: "NY", city: "New York"});
    // ny.add_column("bob", {state: "NY", city: "Jackson Heights"});    
    var expected_mutations = [ 
      { name: 'alice', value: '{"key":"NY","state":"NY","city":"New York"}', timestamp: 'auto' }, 
      { name: 'bob', value: '{"key":"NY","state":"NY","city":"Jackson Heights"}', timestamp: 'auto'}
    ]
    var mutations = ActiveColumns.low_level.mutations_for_save_row_object(
      "ActiveColumnsTest", "StateUsers2", ny)    
    // sys.puts(" --- ny: " + sys.inspect(ny, false, null))
    // sys.puts(" --- mutations: " + sys.inspect(mutations, false, null))
    expected_mutations.forEach(function(exp_mut) {
      assert.ok(_.any(mutations, function(mut) {
        return exp_mut.name == mut.name && exp_mut.value == mut.value && exp_mut.timestamp == mut.timestamp;
      }),
      "Mutations did not contain expected mutation: " + sys.inspect(exp_mut, false, null));
    })
    var exp_mut = expected_mutations[0];
    var mut = ActiveColumns.low_level.mutations_for_save_column_object(null, ny.columns[0])[0];
    assert.ok(exp_mut.name == mut.name && exp_mut.value == mut.value && 
                exp_mut.timestamp == mut.timestamp,
              "Mutations did not contain expected mutation: " + sys.inspect(exp_mut, false, null));
  },
  
  "Mutations for StateLastLoginUsers": function (assert) {
    var StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
    var ny = StateLastLoginUsers.new_object("ny", 
      [
      {
        _name: 2445066, 
        columns: [
          {_name: "alice", city: "New York"}, 
          {_name: "bob", city: "Jackson Heights"}
        ]
      },
      {
        _name: 2445067, 
        columns: [
          {_name: "chuck", city: "Elmhurst"}
        ]
      }                    
    ]);
    // var ny = StateLastLoginUsers.new_object("ny");
    // ny.add_column(2445066).add_column("alice", {city: "New York"});
    // ny.add_column(2445066).add_column("bob", {city: "Jackson Heights"});
    // ny.add_column(2445067).add_column("chuck", {city: "Elmhurst"});
    // sys.puts(" --- ny: " + sys.inspect(ny, false, null));  
    var mutations = ActiveColumns.low_level.mutations_for_save_row_object("ActiveColumnsTest", "StateLastLoginUsers", ny);
    var expected_mutations = [
      {
        "name":2445066,
        "columns":[
          {"name":"alice","value":"{\"key\":\"ny\",\"city\":\"New York\"}","timestamp":"auto"},
          {"name":"bob","value":"{\"key\":\"ny\",\"city\":\"Jackson Heights\"}","timestamp":"auto"}
        ]
      },
      {
        "name":2445067,
        "columns":[{"name":"chuck","value":"{\"key\":\"ny\",\"city\":\"Elmhurst\"}","timestamp":"auto"}]
      }
    ]
    // sys.puts("mutations: " + sys.inspect(mutations, false, null));
    assert.equal(2, mutations.length);        
    expected_mutations.forEach(function(exp_mut) {
      assert.ok(_.any(mutations, function(mut) {
        if (exp_mut.name != mut.name) return false;
        exp_mut.columns.forEach(function(exp_col) {
          assert.ok(_.any(mut.columns, function(col) {
            var val = exp_col.name == col.name && exp_col.value == col.value && exp_col.timestamp == col.timestamp
            return exp_col.name == col.name && exp_col.value == col.value && exp_col.timestamp == col.timestamp;
          }), "Did not get expected column: " + sys.inspect(exp_col, false, null))  
        })
        return true;        
      }), "Did not get expected mutation: " + sys.inspect(exp_mut, false, null))
    })
    
    mutations = ActiveColumns.low_level.mutations_for_save_super_column_object(
      "ActiveColumnsTest", "StateLastLoginUsers", ny.columns[0]);
    assert.equal(1, mutations.length);
    // sys.puts("mutations: " + sys.inspect(mutations, false, null));
    var exp_mut = expected_mutations[0], mut = mutations[0];
    assert.equal(exp_mut.super_column, mut.super_column, 
                 "Mutation did not contain super column " + exp_mut.super_column);
    exp_mut.columns.forEach(function(exp_col) {
      assert.ok(_.any(mut.columns, function(col) {
        if (exp_col.name != col.name || exp_col.timestamp != col.timestamp)
          return false;
        var exp_val = eval('(' + exp_col.value + ')')
        var col_val = eval('(' + col.value + ')')
        for (var k in exp_val) {
        _.forEach(exp_val, function(v, k))
          assert.equal(v, col_val[k], "Expected " + k + " to be " + v);
        }
        return true;
      }), 
      "Mutations for super column " + mut.super_column + 
      " did not contain expected column: " + sys.inspect(exp_col, false, null));
    })
  }
});

suite.runTests();