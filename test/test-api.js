var assert = require('assert');
var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
var logger = log4js.getLogger(logger_name);
logger.setLevel('INFO');
var sys = require('sys')
var _ = require('underscore')._
var async = require('async');

var ActiveColumns = require('active-columns');
ActiveColumns.set_logger(logger);
require('./init-test-keyspace').do_it();

var TestSuite = require('async_testing').TestSuite;

var users1Suite = new TestSuite();
users1Suite.setup(function() {
  this.Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");
});
users1Suite.teardown(function(finished) {
  try {
    var alice = this.alice, bob = this.bob;
    this.Users1.callbacks = {};
    var destroy_alice = alice && alice.destroy;
    var destroy_bob = bob && bob.destroy;
    if (destroy_alice) {
      alice.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying alice." + err);
        else logger.info("Alice destroyed in teardown.");
        finished();
      });
    } 
    if (destroy_bob) {
      bob.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying bob." + err);
        else logger.info("Bob destroyed in teardown.");
        finished();
      });
    }
    if (! (destroy_alice || destroy_bob)) finished();
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
users1Suite.addTests({
  "Test Users1 column family": test_Users1,
});


var users2Suite = new TestSuite();
users2Suite.setup(function() {
  this.Users2 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users2");
});
users2Suite.teardown(function(finished) {
  try {
    var alice = this.alice, bob = this.bob;
    var destroy_alice = alice && alice.destroy;
    var destroy_bob = bob && bob.destroy;
    if (destroy_alice) {
      alice.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying alice." + err);
        else logger.info("Alice destroyed in teardown.");
        finished();
      });
    } 
    if (destroy_bob) {
      bob.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying bob." + err);
        else logger.info("Bob destroyed in teardown.");
        finished();
      });
    }
    if (! (destroy_alice || destroy_bob)) finished();
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
users2Suite.addTests({
  "Test Users2 column family": test_Users2
});


var stateUsersUserLevelSuite = new TestSuite();
stateUsersUserLevelSuite.teardown(function(finished) {
  try {
    this.StateUsersX.callbacks = {};
    var ny_alice = this.ny_alice;
    if (ny_alice && ny_alice.destroy) {
      ny_alice.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying ny_alice." + err);
        else logger.info("ny_alice destroyed in teardown.");
        finished();        
      });
    } else {
      finished();
    }  
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
stateUsersUserLevelSuite.addTests({
  "Test StateUsers1 column family, user level": test_StateUsers1_user_level,
  "Test StateUsers2 column family, user level": test_StateUsers2_user_level
});


var stateUsersStateLevelSuite = new TestSuite();
stateUsersStateLevelSuite.teardown(function(finished) {
  try {
    this.StateUsersX.callbacks = {}
    var ny = this.ny;
    if (ny && ny.destroy) {
      ny.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying ny." + err);
        else logger.info("ny destroyed in teardown.");
        finished();
      });
    } else {
      finished();
    }
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
stateUsersStateLevelSuite.addTests({
  "Test StateUsers1 column family, state level": test_StateUsers1_state_level,
  "Test StateUsers2 column family, state level": test_StateUsers2_state_level
});


var stateLastLoginUsersUserLevelSuite = new TestSuite();
stateLastLoginUsersUserLevelSuite.teardown(function(finished) {
  try {
    this.StateLastLoginUsers.callbacks = {}
    var ny_1271184168_alice = this.ny_1271184168_alice;
    if (ny_1271184168_alice && ny_1271184168_alice.destroy) {
      ny_1271184168_alice.destroy(function(err, result) {
        if(err) assert.ok(false, "error destroying ny_1271184168_alice." + err);
        else logger.info("ny_1271184168_alice destroyed in teardown.");
        finished();
      });
    } else {
      finished();
    }
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
stateLastLoginUsersUserLevelSuite.addTests({
  "Test StateLastLoginUsers column family, user level": test_StateLastLoginUsers_user_level,
});


var stateLastLoginUsersLastLoginLevelSuite = new TestSuite();
stateLastLoginUsersLastLoginLevelSuite.teardown(function(finished) {
  try {
    var ny_1271184168 = this.ny_1271184168;
    this.StateLastLoginUsers.callbacks = {}
    if (ny_1271184168 && ny_1271184168.destroy) {
      ny_1271184168.destroy(function(err, result) {
        if(err) assert.ok(false, "error destroying ny_1271184168." + err);
        else logger.info("ny_1271184168 destroyed in teardown.");
        finished();
      });
    } else {
      finished();
    }
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
stateLastLoginUsersLastLoginLevelSuite.addTests({
  "Test StateLastLoginUsers column family, last login level level": test_StateLastLoginUsers_last_login_level
});


var stateLastLoginUsersStateLevelSuite = new TestSuite();
stateLastLoginUsersStateLevelSuite.teardown(function(finished) {
  try {
    var ny = this.ny;
    this.StateLastLoginUsers.callbacks = {}
    if (ny && ny.destroy) {
      ny.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying ny." + err);
        else logger.info("ny destroyed in teardown.");
        finished();
      });
    } else {
      finished();
    }
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
stateLastLoginUsersStateLevelSuite.addTests({
  "Test StateLastLoginUsers column family, state level": test_StateLastLoginUsers_state_level
});


var columnValueTypeSuite = new TestSuite();
columnValueTypeSuite.teardown(function(finished) {
  try {
    var o = this.o;
    if (o.destroy) {
      o.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying object:" + err);
        else logger.info("Object destroyed in teardown.");
        finished();
      });
    } else {
      finished();
    }
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
columnValueTypeSuite.addTests({
  "Test column value types": test_column_value_types,
  "Test column value types, static column names": test_column_value_types_static
});


var autoIdGenerationSuite = new TestSuite();
autoIdGenerationSuite.teardown(function (finished) {
  try {
    var alice = this.alice, bob = this.bob;
    var destroy_alice = alice && alice.destroy;
    var destroy_bob = bob && bob.destroy;
    if (destroy_alice) {
      alice.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying alice." + err);
        else logger.info("Alice destroyed in teardown.");
        finished();
      });
    } 
    if (destroy_bob) {
      bob.destroy(function(err, result) {
        if (err) assert.ok(false, "error destroying bob." + err);
        else logger.info("Bob destroyed in teardown.");
        finished();
      });
    }
    if (! (destroy_alice || destroy_bob)) finished();
  } catch (e) {
    logger.info("Caught exception trying to destroy() in teardown." + e);
    finished();
  }
});
autoIdGenerationSuite.addTests({  
  "Test auto key generation": test_auto_key_generation
  // "Test auto super column name generation: test_auto_super_column_name_generation,
  // "Test auto column name generation": test_auto_column_name_generation
});

require('async_testing').runSuites({
  "Users1 tests": users1Suite,
  "Users2 tests": users2Suite,
  "StateUsersX tests, user level": stateUsersUserLevelSuite,
  "StateUsersX tests, state level": stateUsersStateLevelSuite,
  "StateLastLoginUsersX tests, user level": stateLastLoginUsersUserLevelSuite,
  "StateLastLoginUsersX tests, last login level": stateLastLoginUsersLastLoginLevelSuite,
  "StateLastLoginUsersX tests, state level": stateLastLoginUsersStateLevelSuite,
  "Column value type tests": columnValueTypeSuite,
  "Auto id generation tests": autoIdGenerationSuite  
});

function test_Users1(assert, finished, test) {

  var Users1 = test.Users1;
  var alice;
  var save_cb_names = ["before_save_row", "after_save_row"];

  function assert_alice(version, city) {
    assert.equal("alice", version.key);
    assert.equal("alice", version.id);
    assert.equal(city, version.city);
    assert.equal("NY", version.state);
    assert.equal(1271184168, version.last_login);
    assert.equal("F", version.sex);       
  }

  function assert_bob() {
    assert.equal("bob", bob.key);
    assert.equal("bob", bob.id);
    assert.equal("Jackson Heights", bob.city);
    assert.equal("NY", bob.state);
    assert.equal("1271184168", bob.last_login);
    assert.equal("M", bob.sex);    
  }

  function find_alice_and_bob(keyspec, next) {
    Users1.find(keyspec, function(err, results) {
      if (err) assert.ok(false, "Error finding bob and alice: " + err);        
      else {
        assert.equal(2, Object.keys(results).length);
        _.forEach(results, function(res, k) {
          if (res.key == "alice") {
            alice = test.alice = res;
            if (isNaN(parseInt(k))) assert.equal("alice", k);
            assert_alice(alice, "Los Angeles");
          } else if (res.key == "bob") {
            bob = test.bob = res;
            if (isNaN(parseInt(k)))  assert.equal("bob", k);
            assert_bob();
          } else {
            assert.ok(false, "Got an unexpected key when finding alice and bob.")
          }
        })
        next();
      }
    })
  }
  
  async.series([
    unsuccessful_find,
    unsuccessful_destroy,
    aborted_save,
    first_save,
    successful_find,
    save_after_find,
    add_bob_to_the_mix,
    find_alice_and_bob_with_range,
    find_alice_and_bob_with_keys,
    successful_destroy
  ],
  finished);

  function unsuccessful_find(next) {
    Users1.find("alice", create_unsuccessful_find_callback("alice", next));    
  }

  function unsuccessful_destroy(next) {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager()
    var cb_token = Math.random();
    var init_cb_names = ["after_initialize_row"];
    tcm.add(Users1, init_cb_names, cb_token);
    alice = test.alice = Users1.new_object("alice", {
     city: "New York", state: "NY", last_login: 1271184168, sex: "F"
    });
    tcm.assert(init_cb_names, cb_token);
    try {
      alice.destroy();
      assert.ok(false, "Expected destroy for alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for alice threw exception as expected: " + e);
      next();
    }
  }

  function aborted_save(next) {
    _aborted_save(Users1, "row", alice, "alice",  function() {
      unsuccessful_find(next);
    });
  }

  function first_save(next) {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(Users1, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      Users1.add_callback(cb_name, function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert.strictEqual(alice, this);
        cb_finished();
      });
    });
    alice.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save alice: " + err);
      else {
       logger.info("Alice saved successfully.");
       tcm.assert(save_cb_names, cb_token);
       next();
     }
    });    
  }

  function successful_find(next) {
    var cb_token = Math.random();
    var find_cb_names = ["after_find_row", "after_initialize_row"]
    var tcm = tokenCallbackManager();
    tcm.add(Users1, find_cb_names, cb_token);
    Users1.find("alice", function(err, result) {
      if (err) assert.ok(false, "Error looking for alice:" + err );
      else if (!result) assert.ok(false, "Could not find alice.");
      else {
        alice = test.alice = result;
        assert_alice(alice, "New York");
        assert_alice(alice._last_saved, "New York");
        tcm.assert(find_cb_names, cb_token);
        logger.info("Alice found successfully and save result validated.");
        next();
      }
    });

  }

  function save_after_find(next) {
    alice.city = "Los Angeles";
    assert_alice(alice, "Los Angeles");
    assert_alice(alice._last_saved, "New York");
    Users1.callbacks = {}
    var cb_token = Math.random();;
    var tcm = tokenCallbackManager();
    tcm.add(Users1, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      Users1.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_alice(previous_version, "New York");
        assert.strictEqual(alice, this);
        cb_finished();
      });
    });
    alice.save(function(err, result) {
      if (err) assert.ok(false, "Error saving alice: " + err);        
      else {
        tcm.assert(save_cb_names, cb_token);
        Users1.find("alice", function(err, result) {
          if (err) assert.ok(false, "Error finding alice.");
          else {
            logger.info("Alice saved successfully after find.");
            alice = test.alice = result;
            assert.equal("Los Angeles", alice.city);
            Users1.callbacks = {}
            next();
          }
        });
      }
    });
  }

  function add_bob_to_the_mix(next) {
    bob = test.bob = Users1.new_object("bob", {
     city: "Jackson Heights", state: "NY", last_login: 1271184168, sex: "M"
    });
    bob.save(function(err, result) {
      if (err) assert.ok(false, "Error saving bob: " + err);        
      else {
        logger.info("Saved bob successfully.");
        next();
      }
    });
  }

  function find_alice_and_bob_with_range(next) {
    find_alice_and_bob({start_key:'', end_key:'', count: 100}, function() {
      logger.info("Found alice and bob with range successfully.");
      next();
    });
  }

  function find_alice_and_bob_with_keys(next) {
    find_alice_and_bob(['alice', 'bob'], function() {
      logger.info("Found alice and bob with keys successfully.");
      next();      
    }); 
  }

  function successful_destroy(next) {
    try {
      var cb_token = Math.random();;
      Users1.add_callback("after_destroy_row", function(previous_version, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(alice, this);
        cb_finished();
      });
      alice.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying alice: " + err);
        else {
          Users1.callbacks = {}
          assert.equal(cb_token, alice.after_destroy_token);
          logger.info("Successfully destroyed alice.");
          unsuccessful_find(next);
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for alice threw an exception.");
    }    
  }
      
}

function test_Users2(assert, finished, test) {
  
  var Users2 = test.Users2;
    
  var predicate = {column_names:["city", "state", "last_login", "sex"]};
  var alice_columns = [
   {name: "city", value: "New York"},
   {name: "state", value:"NY"}, 
   {name: "last_login", value: 1271184168}, 
   {name: "sex", value: "F"}
  ];
  var bob_columns = [
   {name: "city", value: "New York"},
   {name: "state", value:"NY"}, 
   {name: "last_login", value: 1271184168}, 
   {name: "sex", value: "F"}
  ];
  var alice, bob;
  
  start();
    
  function start() { unsuccessful_find(unsuccessful_destroy); };
  
  function unsuccessful_find(not_found_action) {
    Users2.find("alice", predicate, create_unsuccessful_find_callback("alice", not_found_action))    
  }
  
  function unsuccessful_destroy() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var init_cb_names = ["after_initialize_row"];
    tcm.add(Users2, init_cb_names, cb_token);
    alice = test.alice = Users2.new_object("alice", alice_columns);
    tcm.assert(init_cb_names, cb_token);
    try {
      alice.destroy();
      assert.ok(false, "Expected destroy for alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for alice threw exception as expected: " + e);
      aborted_save();
    }
  }
  
  function aborted_save() {
    _aborted_save(Users2, "row", alice, "alice", function() {
      unsuccessful_find(first_save);
    });
  }
  
  var save_cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(Users2, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      Users2.add_callback(cb_name, function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert.strictEqual(alice, this);
        cb_finished();
      });
    });
    alice.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save alice: " + err);
      else {
       logger.info("Alice saved successfully.");
       tcm.assert(save_cb_names, cb_token);
       successful_find();
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var find_cb_names = ["after_find_row", "after_initialize_row"];
    tcm.add(Users2, find_cb_names, cb_token);
    Users2.find("alice", predicate, function(err, result) {
      if (err) assert.ok(false, "Error looking for alice:" + err );
      else if (!result) assert.ok(false, "Could not find alice.");
      else {
        alice = test.alice = result;
        assert_alice(alice, "New York");
        tcm.assert(find_cb_names, cb_token);
        logger.info("Alice found successfully and save result validated.");
        save_after_find();
      }
    });
    
  }

  function assert_alice(version, city) {
    alice_columns.forEach(function(exp_col) {
      assert.ok(_.any(version.columns, function(col) {
        if (col.name == "city") return col.value == city;
        else return exp_col.name == col.name && exp_col.value == col.value;
      }), 
      "Columns or city do not match.  alice_columns: " + 
        sys.inspect(alice_columns, false, true) + ", version.columns: " +
        sys.inspect(version.columns, false, true) + ", city: " + city
      )
    });
  }

  function save_after_find() {
    _.detect(alice.columns, function(col) {return col.name == "city"}).value = "Los Angeles";
    Users2.callbacks = {}
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(Users2, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      Users2.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_alice(previous_version, "New York");
        assert.strictEqual(alice, this);
        cb_finished();
      });
    });
    alice.save(function(err, result) {
      if (err) assert.ok(false, "Error saving alice: " + err);        
      else {
        tcm.assert(save_cb_names, cb_token);
        logger.info("Alice saved successfully after find.");
        Users2.find("alice", predicate, function(err, result) {
          if (err) assert.ok(false, "Error finding alice.");
          else {
            alice = test.alice = result;
            assert_alice(alice, "Los Angeles");
            logger.info("Save after find validated.");
            Users2.callbacks = {};
            add_bob_to_the_mix();
          }
        });
      }
    });
  }
   
  function add_bob_to_the_mix() {
    bob = test.bob = Users2.new_object("bob", bob_columns);
    bob.save(function(err, result) {
      if (err) assert.ok(false, "Error saving bob: " + err);        
      else {
        logger.info("Saved bob successfully.");
        find_alice_and_bob();
      }
    });
  }
  
  function find_alice_and_bob() {
    Users2.find({start_key:'', end_key:'', count: 100}, predicate, function(err, results) {
      if (err) assert.ok(false, "Error finding bob and alice: " + err);        
      else {
        assert.equal(2, results.length);
        results.forEach(function(res) {
          if (res.key == "alice") {
            alice = test.alice = res;
            assert_alice(alice, "Los Angeles");
          } else if (res.key == "bob") {
            bob = test.bob = res;
            assert_bob();
          } else {
            assert.ok(false, "Got an unexpected key from find with key range.")
          }
        })
        successful_destroy();
      }
    })
  }
  
  function assert_bob() {
    assert.equal("bob", bob.key);
    assert.equal("bob", bob.id);
    bob_columns.forEach(function(exp_col) {
      assert.ok(_.any(bob.columns, function(col) {
        return exp_col.name == col.name && exp_col.value == col.value;
      }))          
    });
  }
      
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      Users2.add_callback("after_destroy_row", function(previous_version, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(alice, this);
        cb_finished();
      });
      alice.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying alice: " + err);
        else {
          logger.info("Successfully destroyed alice.")
          assert.equal(cb_token, alice.after_destroy_token);
          Users2.callbacks = {};
          unsuccessful_find(finished);
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for alice threw an exception.");
    }    
  }
    
}

function test_StateUsers1_user_level(assert, finished, test) {
  _test_StateUsersX_user_level(assert, finished, test, "StateUsers1")
}

function test_StateUsers1_state_level(assert, finished, test) {
  _test_StateUsersX_state_level(assert, finished, test, "StateUsers1")
}

function test_StateUsers2_user_level(assert, finished, test) {
  _test_StateUsersX_user_level(assert, finished, test, "StateUsers2")
}

function test_StateUsers2_state_level(assert, finished, test) {
  _test_StateUsersX_state_level(assert, finished, test, "StateUsers2")
}

function _test_StateUsersX_user_level(assert, finished, test, column_family) {

  var StateUsersX = test.StateUsersX = ActiveColumns.get_column_family("ActiveColumnsTest", column_family);
  var callback_level = column_family == "StateUsers1" ? "super_column" : "column"
  
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var alice_new_city = "Los Angeles"
  var ny_alice;
  
  start();
  
  function start() { unsuccessful_find(unsuccessful_destroy); };
  
  function unsuccessful_find(not_found_action) {
    StateUsersX.find("NY", "alice", create_unsuccessful_find_callback("ny_alice", not_found_action))    
  }
  
  function unsuccessful_destroy() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var init_cb_names = ["after_initialize_" + callback_level];
    tcm.add(StateUsersX, init_cb_names, cb_token);
    ny_alice = test.ny_alice = StateUsersX.new_object("NY", "alice", alice_value);
    tcm.assert(init_cb_names, cb_token);
    try {
      ny_alice.destroy();
      assert.ok(false, "Expected destroy for ny_alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny_alice threw exception as expected: " + e);
      aborted_save();
    }
  }
  
  function aborted_save() {
    _aborted_save(StateUsersX, callback_level, ny_alice, "ny_alice", function() {
      unsuccessful_find(first_save);
    });
  }

  var save_cb_names = ["before_save_" + callback_level, "after_save_" + callback_level];
  
  function first_save() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(StateUsersX, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert_ny_alice(ny_alice, "New York");
        cb_finished();
      });
    });
    ny_alice.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save ny_alice: " + err);
      else {
       logger.info("ny_alice saved successfully.");
       tcm.assert(save_cb_names, cb_token);
       successful_find();
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var find_cb_names = ["after_find_" + callback_level, "after_initialize_" + callback_level];
    tcm.add(StateUsersX, find_cb_names, cb_token);
    StateUsersX.find("NY", "alice", function(err, result) {
      if (err) assert.ok(false, "Error looking for ny_alice:" + err );
      else if (!result) assert.ok(false, "Could not find ny_alice.");
      else {
        ny_alice = test.ny_alice = result;
        assert_ny_alice(ny_alice, "New York");
        tcm.assert(find_cb_names, cb_token);
        logger.info("ny_alice found successfully and save result validated.");
        save_after_find();
      }
    });
    
  }
  
  function assert_ny_alice(version, city) {
    assert.equal(alice_value._name, version.id);
    assert.equal(alice_value._name, version._name);
    assert.equal(city, version.city);
    assert.equal(alice_value.sex, version.sex);
  }

  function save_after_find() {
    var prev_city = ny_alice.city;
    var cb_token = Math.random();
    StateUsersX.callbacks = {};
    var tcm = tokenCallbackManager();
    tcm.add(StateUsersX, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_ny_alice(previous_version, prev_city);
        assert.strictEqual(ny_alice, this);
        cb_finished();
      });
    });
    ny_alice.city = alice_new_city
    ny_alice.save(function(err, result) {
      if (err) assert.ok(false, "Error saving ny_alice: " + err);        
      else {
        tcm.assert(save_cb_names, cb_token);
        StateUsersX.find("NY", "alice", function(err, result) {
          if (err) assert.ok(false, "Error finding ny_alice.");
          else {
            ny_alice = test.ny_alice = result;
            assert_ny_alice(ny_alice, alice_new_city);
            logger.info("ny_alice saved successfully and result validated after find.");
            successful_destroy();
          }
        });
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateUsersX.add_callback("after_destroy_" + callback_level, function(previous_version, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(ny_alice, this);
        cb_finished();
      });
      ny_alice.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying ny_alice: " + err);
        else {
          StateUsersX.callbacks = {}
          assert.equal(cb_token, ny_alice.after_destroy_token);
          logger.info("Successfully destroyed ny_alice.")
          unsuccessful_find(finished);
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny_alice threw an exception.");
    }    
  }
    
}

function _test_StateUsersX_state_level(assert, finished, test, column_family) {

  var StateUsersX = test.StateUsersX = ActiveColumns.get_column_family("ActiveColumnsTest", column_family);
  
  var column_predicate = {slice_range:{start:'a', finish: 'c', reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var bob_value = {_name: "bob", city: "Jackson Heights", sex: "M" };
  var alice_new_city = "Los Angeles"
  var bob_new_city = "San Francisco"
  var ny;
  
  start();
  
  function start() { unsuccessful_find(unsuccessful_destroy); };
  
  function unsuccessful_find(not_found_action) {
    StateUsersX.find("NY", column_predicate, create_unsuccessful_find_callback("ny", not_found_action))    
  }
  
  function unsuccessful_destroy() {
    var tcm = tokenCallbackManager();
    var cb_token = Math.random();
    var init_cb_names = ["after_initialize_row"]
    tcm.add(StateUsersX, init_cb_names, cb_token);
    ny = test.ny = StateUsersX.new_object("NY", [
      alice_value,
      bob_value
    ]);
    tcm.assert(init_cb_names, cb_token);
    try {
      ny.destroy();
      assert.ok(false, "Expected destroy for ny to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny threw exception as expected: " + e);
      aborted_save();
    }
  }
  
  function aborted_save() {
    _aborted_save(StateUsersX, "row", ny, "ny", function() {
      unsuccessful_find(first_save);
    });
    
  }
    
  var save_cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(StateUsersX, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert.strictEqual(ny, this);
        cb_finished();
      });
    });
    ny.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save ny: " + err);
      else {
       logger.info("ny saved successfully.");
       tcm.assert(save_cb_names, cb_token);
       successful_find();
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var find_cb_names = ["after_find_row", "after_initialize_row"]
    tcm.add(StateUsersX, find_cb_names, cb_token);
    StateUsersX.find("NY", column_predicate, function(err, result) {
      if (err) assert.ok(false, "Error looking for ny:" + err );
      else if (!result) assert.ok(false, "Could not find ny.");
      else {      
        ny = test.ny = result;
        assert.equal(2, ny.columns.length);
        tcm.assert(find_cb_names, cb_token);
        ny_alice = test.ny_alice = ny.columns[0];
        ny_bob = test.ny_bob = ny.columns[1];
        assert_ny_alice(ny_alice, "New York");
        assert_ny_bob(ny_bob, "Jackson Heights");
        logger.info("ny found successfully and save result validated.");
        save_after_find();
      }
    });
    
  }
  
  function assert_ny_alice(version, city) {
    assert.equal("alice", version.id);
    assert.equal("alice", version._name);
    assert.equal(city, version.city);
    assert.equal(alice_value.sex, version.sex);
  }

  function assert_ny_bob(version, city) {
    assert.equal("bob", version.id);
    assert.equal("bob", version._name);
    assert.equal(city, version.city);
    assert.equal(bob_value.sex, version.sex);
  }

  function save_after_find() {
    var prev_city = alice_value.city;
    ny.columns[0].city = alice_new_city
    ny.columns[1].city = bob_new_city
    var cb_token = Math.random();
    StateUsersX.callbacks = {}
    var tcm = tokenCallbackManager();
    tcm.add(StateUsersX, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_ny_alice(previous_version.columns[0], prev_city);
        assert.strictEqual(ny, this);
        cb_finished();
      });
    });
    ny.save(function(err, result) {
      if (err) assert.ok(false, "Error finding ny.");
      else {
        tcm.assert(save_cb_names, cb_token);
        StateUsersX.find("NY", column_predicate, function(err, result) {
          if (err) assert.ok(false, "Error finding ny.");
          else {
            ny = test.ny = result;
            ny_alice = test.ny_alice = result.columns[0];
            ny_bob = test.ny_bob = result.columns[1];
            assert_ny_alice(ny_alice, alice_new_city);
            assert_ny_bob(ny_bob, bob_new_city);
            logger.info("ny saved successfully and result validated after find.");
            successful_destroy();
          }
        });
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateUsersX.add_callback("after_destroy_row", function(previous_version, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(ny, this);
        cb_finished();
      });
      ny.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying ny: " + err);
        else {        
          logger.info("Successfully destroyed ny.")
          StateUsersX.callbacks = {}
          assert.equal(cb_token, ny.after_destroy_token);
          unsuccessful_find(finished);
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny threw an exception.");
    }    
  }
    
}
    
function test_StateLastLoginUsers_user_level(assert, finished, test) {

  var StateLastLoginUsers = test.StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
  
  var column_predicate = {slice_range:{start:'', finish: '', reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var alice_new_city = "Los Angeles";
  var ny_1271184168_alice;
  
  start();
  
  function start() { unsuccessful_find(unsuccessful_destroy); };
  
  function unsuccessful_find(not_found_action) {
    StateLastLoginUsers.find("NY", 1271184168, "alice", 
      create_unsuccessful_find_callback("ny_1271184168_alice", not_found_action));
  }
  
  function unsuccessful_destroy() {
    var tcm = tokenCallbackManager();
    var init_cb_names = ["after_initialize_column"]
    var cb_token = Math.random();
    tcm.add(StateLastLoginUsers, init_cb_names, cb_token);
    ny_1271184168_alice = test.ny_1271184168_alice = StateLastLoginUsers.new_object("NY", 1271184168, "alice", alice_value);
    tcm.assert(init_cb_names, cb_token);
    try {
      ny_1271184168_alice.destroy();
      assert.ok(false, "Expected destroy for ny_1271184168_alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny_1271184168_alice threw exception as expected: " + e);
      aborted_save();
    }
  }
  
  function aborted_save() {
    _aborted_save(StateLastLoginUsers, "column", ny_1271184168_alice, "ny_1271184168_alice", 
                  function() {
                    unsuccessful_find(first_save);
                  });
  }
    
  var save_cb_names = ["before_save_column", "after_save_column"];
  
  function first_save() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(StateLastLoginUsers, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert_ny_1271184168_alice(this, "New York");
        cb_finished();
      });
    });
    ny_1271184168_alice.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save ny_1271184168_alice: " + err);
      else {      
       logger.info("ny_1271184168_alice saved successfully.");
       tcm.assert(save_cb_names, cb_token);
       successful_find();
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var find_cb_names = ["after_find_column", "after_initialize_column"];
    tcm.add(StateLastLoginUsers, find_cb_names, cb_token);
    StateLastLoginUsers.find("NY", 1271184168, "alice", function(err, result) {
      if (err) assert.ok(false, "Error looking for ny_1271184168_alice:" + err );
      else if (!result) assert.ok(false, "Could not find ny_1271184168_alice.");
      else {
        ny_1271184168_alice = test.ny_1271184168_alice =  result;
        assert_ny_1271184168_alice(ny_1271184168_alice, "New York");
        tcm.assert(find_cb_names, cb_token);
        logger.info("ny_1271184168_alice found successfully and save result validated.");
        save_after_find();
      }
    });
    
  }
  
  function assert_ny_1271184168_alice(version, city) {
    assert.equal(alice_value._name, version.id);
    assert.equal(alice_value._name, version._name);
    assert.equal(city, version.city);
    assert.equal(alice_value.sex, version.sex);
  }

  function save_after_find() {
    var prev_city = ny_1271184168_alice.city; 
    var cb_token = Math.random();
    StateLastLoginUsers.callbacks = {}
    var tcm = tokenCallbackManager();
    tcm.add(StateLastLoginUsers, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_ny_1271184168_alice(previous_version, prev_city);
        assert_ny_1271184168_alice(this, alice_new_city);
        cb_finished();
      });
    });
    ny_1271184168_alice.city = alice_new_city
    ny_1271184168_alice.save(function(err, result) {
      if (err) assert.ok(false, "Error saving ny_1271184168_alice: " + err);        
      else {
        tcm.assert(save_cb_names, cb_token);
        StateLastLoginUsers.find("NY", 1271184168, "alice", function(err, result) {
          if (err) assert.ok(false, "Error finding ny_1271184168_alice.");
          else {
            ny_1271184168_alice = test.ny_1271184168_alice = result;
            assert_ny_1271184168_alice(ny_1271184168_alice, alice_new_city);
            logger.info("ny_1271184168_alice saved successfully and result validated after find.");
            successful_destroy();
          }
        });
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateLastLoginUsers.add_callback("after_destroy_column", function(previous_version, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(ny_1271184168_alice, this);
        cb_finished();
      });
      ny_1271184168_alice.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying ny_1271184168_alice: " + err);
        else {
          logger.info("Successfully destroyed ny_1271184168_alice.")
          StateLastLoginUsers.callbacks = {}
          assert.equal(cb_token, ny_1271184168_alice.after_destroy_token);
          unsuccessful_find(finished);
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny_1271184168_alice threw an exception.");
    }    
  }
    
}

function test_StateLastLoginUsers_last_login_level(assert, finished, test) {

  var StateLastLoginUsers = test.StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
  
  var column_predicate = {slice_range:{start:'', finish: '', reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var bob_value = {_name: "bob", city: "Jackson Heights", sex: "M" };
  var alice_new_city = "Los Angeles"
  var bob_new_city = "San Francisco"
  var ny_1271184168;
  
  start();
  
  function start() { unsuccessful_find(unsuccessful_destroy); };
  
  function unsuccessful_find(not_found_action) {
    StateLastLoginUsers.find("NY", 1271184168, column_predicate,
      create_unsuccessful_find_callback("ny_1271184168", not_found_action))    
  }
  
  function unsuccessful_destroy() {
    var tcm = tokenCallbackManager();
    var init_cb_names = ["after_initialize_super_column"];
    var cb_token = Math.random();
    tcm.add(StateLastLoginUsers, init_cb_names, cb_token);
    ny_1271184168 = test.ny_1271184168 = StateLastLoginUsers.new_object("NY", 1271184168, [
      bob_value,
      alice_value
    ]);
    tcm.assert(init_cb_names, cb_token);
    try {
      ny_1271184168.destroy();
      assert.ok(false, "Expected destroy for ny_1271184168 to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny_1271184168 threw exception as expected: " + e);
      aborted_save();
    }
  }
  
  function aborted_save() {
    _aborted_save(StateLastLoginUsers, "super_column", ny_1271184168, "ny_1271184168", 
                  function() { unsuccessful_find(first_save); });
  }
  
  var save_cb_names = ["before_save_super_column", "after_save_super_column"];
  
  function first_save() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(StateLastLoginUsers, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert_ny_1271184168(this, 1, "New York", 0, "Jackson Heights");
        cb_finished();
      });
    });
    ny_1271184168.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save ny_1271184168: " + err);
      else {
       tcm.assert(save_cb_names, cb_token);
       logger.info("ny_1271184168 saved successfully.");
       successful_find();
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var find_cb_names = ["after_find_super_column", "after_initialize_super_column"]
    tcm.add(StateLastLoginUsers, find_cb_names, cb_token);
    StateLastLoginUsers.find("NY", 1271184168, column_predicate, function(err, result) {
      if (err) assert.ok(false, "Error looking for ny_1271184168:" + err );
      else if (!result) assert.ok(false, "Could not find ny_1271184168.");
      else {
        ny_1271184168 = test.ny_1271184168 = result;
        assert_ny_1271184168(ny_1271184168, 0, "New York", 1, "Jackson Heights");
        tcm.assert(find_cb_names, cb_token);
        logger.info("ny_1271184168 found successfully and save result validated.");
        save_after_find();
      }
    });
    
  }
  
  function assert_ny_1271184168(version, alice_index, alice_city, bob_index, bob_city) {
    assert.equal(1271184168, version.id);
    assert.equal(1271184168, version._name);
    assert.equal(2, ny_1271184168.columns.length);
    var alice_version = version.columns[alice_index];
    assert.equal(alice_value._name, alice_version.id);
    assert.equal(alice_value._name, alice_version._name);
    assert.equal(alice_city, alice_version.city);
    assert.equal(alice_value.sex, alice_version.sex);
    var bob_version = version.columns[bob_index];
    assert.equal(bob_value._name, bob_version.id);
    assert.equal(bob_value._name, bob_version._name);
    assert.equal(bob_city, bob_version.city);
    assert.equal(bob_value.sex, bob_version.sex);
  }
  

  function save_after_find() {
    var alice_prev_city = ny_1271184168.columns[0].city;
    var bob_prev_city = ny_1271184168.columns[1].city;
    var cb_token = Math.random();
    StateLastLoginUsers.callbacks = {}
    var tcm = tokenCallbackManager();
    tcm.add(StateLastLoginUsers, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_ny_1271184168(previous_version, 0, alice_prev_city, 1, bob_prev_city);
        assert_ny_1271184168(this, 0, alice_new_city, 1, bob_new_city);
        cb_finished();
      });
    });
    ny_1271184168.columns[0].city = alice_new_city
    ny_1271184168.columns[1].city = bob_new_city
    ny_1271184168.save(function(err, result) {
      if (err) assert.ok(false, "Error saving ny_1271184168: " + err);        
      else {
        tcm.assert(save_cb_names, cb_token);
        StateLastLoginUsers.find("NY", 1271184168, column_predicate, function(err, result) {
          if (err) assert.ok(false, "Error finding ny_1271184168.");
          else {
            ny_1271184168 = test.ny_1271184168 = result;
            assert_ny_1271184168(ny_1271184168, 0, alice_new_city, 1, bob_new_city);
            logger.info("ny_1271184168 saved successfully and result validated after find.");
            successful_destroy();
          }
        });
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateLastLoginUsers.add_callback("after_destroy_super_column", function(previous_version, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(ny_1271184168, this);
        cb_finished();
      });
      ny_1271184168.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying ny_1271184168: " + err);
        else {
          logger.info("Successfully destroyed ny_1271184168.")
          StateLastLoginUsers.callbacks = {}
          assert.equal(cb_token, ny_1271184168.after_destroy_token);
          unsuccessful_find( finished );
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny_1271184168 threw an exception.");
    }    
  }
    
}

function test_StateLastLoginUsers_state_level(assert, finished, test) {

  var ny, ny_1271184168, ny_1271184169;
  var StateLastLoginUsers = test.StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
  
  var column_predicate = {slice_range:{start:1271184168, finish: 1271184169, reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var bob_value = {_name: "bob", city: "Jackson Heights", sex: "M" };
  var alice_new_city = "Los Angeles"
  var bob_new_city = "San Francisco"
  var chuck_value = {_name: "chuck", city: "Elmhurst", sex: "M" };
  var dave_value = {_name: "dave", city: "Brooklyn", sex: "F" };
  var chuck_new_city = "Seattle"
  var dave_new_city = "Portland"
  
  start();
  
  function start() { unsuccessful_find(unsuccessful_destroy); };
  
  function unsuccessful_find(not_found_action) {
    StateLastLoginUsers.find("NY", column_predicate, 
      create_unsuccessful_find_callback("ny", not_found_action))    
  }
  
  function unsuccessful_destroy() {
    var tcm = tokenCallbackManager();
    var init_cb_names = ["after_initialize_row"];
    var cb_token = Math.random();
    tcm.add(StateLastLoginUsers, init_cb_names, cb_token);
    ny = test.ny = StateLastLoginUsers.new_object("NY", [
      {_name: 1271184169, columns:[dave_value, chuck_value]},
      {_name: 1271184168, columns:[bob_value, alice_value]}
    ]);
    tcm.assert(init_cb_names, cb_token);
    try {
      ny.destroy();
      assert.ok(false, "Expected destroy for ny to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny threw exception as expected: " + e);
      aborted_save();
    }
  }
  
  function aborted_save() {
    _aborted_save(StateLastLoginUsers, "row", ny, "ny", 
                  function() {
                    unsuccessful_find(first_save);
                  });
  }
  
  var save_cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    tcm.add(StateLastLoginUsers, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, function(previous_version, cb_finished) {
        assert.equal(null, previous_version);
        assert.strictEqual(ny, this);
        cb_finished();
      });
    });
    ny.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save ny: " + err);
      else {
       tcm.assert(save_cb_names, cb_token);
       logger.info("ny saved successfully.");
       successful_find();
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    var tcm = tokenCallbackManager();
    var find_cb_names = ["after_find_row", "after_initialize_row"]
    tcm.add(StateLastLoginUsers, find_cb_names, cb_token);
    StateLastLoginUsers.find("NY", column_predicate, function(err, result) {
      if (err) assert.ok(false, "Error looking for ny:" + err );
      else if (!result) assert.ok(false, "Could not find ny.");
      else {
        ny = test.ny = result;
        assert.equal(2, ny.columns.length);
        tcm.assert(find_cb_names, cb_token);
        ny_1271184168 = test.ny_1271184168 = ny.columns[0];
        ny_1271184169 = ny.columns[1];
        assert_ny_1271184168(ny_1271184168, 0, "New York", 1, "Jackson Heights");
        assert_ny_1271184169(ny_1271184169, 0, "Elmhurst", 1, "Brooklyn");
        logger.info("ny found successfully and save result validated.");
        save_after_find();
      }
    });
    
  }
  
  function assert_ny_1271184168(version, alice_index, alice_city, bob_index, bob_city) {
    assert.equal(1271184168, version.id);
    assert.equal(1271184168, version._name);
    assert.equal(2, version.columns.length);
    var alice_version = version.columns[alice_index];
    assert.equal(alice_value._name, alice_version.id);
    assert.equal(alice_value._name, alice_version._name);
    assert.equal(alice_city, alice_version.city);
    assert.equal(alice_value.sex, alice_version.sex);
    var bob_version = version.columns[bob_index];
    assert.equal(bob_value._name, bob_version.id);
    assert.equal(bob_value._name, bob_version._name);
    assert.equal(bob_city, bob_version.city);
    assert.equal(bob_value.sex, bob_version.sex);
  }

  function assert_ny_1271184169(version, chuck_index, chuck_city, dave_index, dave_city) {
    assert.equal(1271184169, version.id);
    assert.equal(1271184169, version._name);
    assert.equal(2, version.columns.length);
    var chuck_version = version.columns[chuck_index];
    assert.equal(chuck_value._name, chuck_version.id);
    assert.equal(chuck_value._name, chuck_version._name);
    assert.equal(chuck_city, chuck_version.city);
    assert.equal(chuck_value.sex, chuck_version.sex);
    var dave_version = version.columns[dave_index];
    assert.equal(dave_value._name, dave_version.id);
    assert.equal(dave_value._name, dave_version._name);
    assert.equal(dave_city, dave_version.city);
    assert.equal(dave_value.sex, dave_version.sex);
  }

  function save_after_find() {
    var alice_prev_city = ny.columns[0].columns[0].city;
    var bob_prev_city = ny.columns[0].columns[1].city;
    var chuck_prev_city = ny.columns[1].columns[0].city;
    var dave_prev_city = ny.columns[1].columns[1].city;
    var cb_token = Math.random();
    StateLastLoginUsers.callbacks = {}
    var tcm = tokenCallbackManager();
    tcm.add(StateLastLoginUsers, save_cb_names, cb_token);
    save_cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, function(previous_version, cb_finished) {
        assert_ny_1271184168(previous_version.columns[0], 0, alice_prev_city, 1, bob_prev_city);
        assert_ny_1271184169(previous_version.columns[1], 0, chuck_prev_city, 1, dave_prev_city);
        assert.strictEqual(ny, this);
        cb_finished();
      });
    });
    ny.columns[0].columns[0].city = alice_new_city
    ny.columns[0].columns[1].city = bob_new_city
    ny.columns[1].columns[0].city = chuck_new_city
    ny.columns[1].columns[1].city = dave_new_city
    ny.save(function(err, result) {
      if (err) assert.ok(false, "Error saving ny: " + err);        
      else {
        tcm.assert(save_cb_names, cb_token);
        StateLastLoginUsers.find("NY", column_predicate, function(err, result) {
          if (err) assert.ok(false, "Error finding ny.");
          else {
            ny = test.ny = result;
            ny_1271184168 = test.ny_1271184168 = result.columns[0];
            ny_1271184169 = result.columns[1];
            assert_ny_1271184168(ny_1271184168, 0, alice_new_city, 1, bob_new_city);
            assert_ny_1271184169(ny_1271184169, 0, chuck_new_city, 1, dave_new_city);
            logger.info("ny saved successfully and result validated after find.");
            successful_destroy();
          }
        });
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateLastLoginUsers.add_callback("after_destroy_row", function(event_listeners, cb_finished) {
        this.after_destroy_token = cb_token;
        assert.strictEqual(ny, this);
        cb_finished();
      });
      ny.destroy(function(err) {
        if (err) assert.ok(false, "Error destroying ny: " + err);
        else {
          logger.info("Successfully destroyed ny.")
          StateLastLoginUsers.callbacks = {}
          assert.equal(cb_token, ny.after_destroy_token);
          unsuccessful_find( finished );
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny threw an exception.");
    }    
  }
    
}

function test_column_value_types(assert, finished, test) {
  var ColumnValueTypeTest = ActiveColumns.get_column_family("ActiveColumnsTest", "ColumnValueTypeTest");  
  var date_val = new Date();
  var number_val = Math.random();
  var json_val = {foo: 'boo', bar: 10};
  var o = test.o = ColumnValueTypeTest.new_object([
   {name: "date_col", value: date_val},
   {name: "number_col", value: number_val}
  ]);
  _test_column_value_types(ColumnValueTypeTest, o, function(result) {
    var date_col = _.detect(result.columns, function(col) {return col.name == "date_col";});
    assert.ok(date_col.value.valueOf, 
              "Doesn't look like a date object, no valueOf() method");
    assert.equal(date_val.valueOf(), date_col.value.valueOf());
    var number_col = _.detect(result.columns, function(col) {return col.name == "number_col";});
    assert.equal("number", typeof number_col.value);
    assert.equal(number_val, number_col.value);    
  }, assert, finished, test);
}

function test_column_value_types_static(assert, finished, test) {
  var ColumnValueTypeTestStatic = ActiveColumns.get_column_family("ActiveColumnsTest", "ColumnValueTypeTestStatic");  
  var date_val = new Date();
  var number_val = Math.random();
  var json_val = {foo: 'boo', bar: 10};
  var o = test.o = ColumnValueTypeTestStatic.new_object({
   date_col: date_val, number_col: number_val, json_col: json_val
  });
  _test_column_value_types(ColumnValueTypeTestStatic, o, function(result) {
    assert.ok(result.date_col.valueOf, 
              "Doesn't look like a date object, no valueOf() method");
    assert.equal(date_val.valueOf(), result.date_col.valueOf());
    assert.equal("number", typeof result.number_col);
    assert.equal(number_val, result.number_col);
    assert.equal(json_val.foo, result.json_col.foo);
    assert.equal(json_val.bar, result.json_col.bar);
  }, assert, finished, test);
}

function _test_column_value_types(cf, o, assert_func, assert, finished, test) {
  o.save(function(err, id) {
    if (err) assert.ok(false, "Error trying to save object: " + err);
    else {
     cf.find(id, {column_names: ["date_col", "number_col", "json_col"]}, function(err, result) {
       if (err) assert.ok(false, "Error trying to find object: " + err)
       else {
         o = test.o = result;
         assert_func(result);
         logger.info("Object successfully returned from find with correct column value types.")
         finished();
       } 
     });
   }
  });
}

function test_auto_key_generation(assert, finished, test) {
  var alice, bob;
  var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");
  var uuid_regex = /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$/;
  
  alice = test.alice = Users1.new_object({
   city: "New York", state: "NY", last_login: 1271184168, sex: "F"
  });
  alice.save(function(err, result) {
    if (err) assert.ok(false, "Error trying to save alice: " + err);
    else {    
     assert.ok(result.match(uuid_regex),
               "Save for alice failed to return a UUID.");
     assert.equal(result, alice.key,
                  "Save for alice failed to set key property.");
     logger.info("Save for alice successfully returned a UUID.")
     do_bob_now();
   }
  });
  
  function do_bob_now() {
    bob = test.bob = Users1.new_object(null, {
     city: "Jackson Heights", state: "NY", last_login: 1271184169, sex: "M"
    });
    bob.save(function(err, result) {
      if (err) assert.ok(false, "Error trying to save bob: " + err);
      else {      
       assert.ok(result.match(uuid_regex),
                 "Save for bob failed to return a UUID.");
       assert.equal(result, bob.key,
                    "Save for bob failed to set key property.");
       logger.info("Save for bob successfully returned a UUID.");
       finished();
     }
    });    
  }
}


function create_unsuccessful_find_callback(object_name, not_found_action) {
  return function(err, result) {
    if (err) {
      assert.ok(false, "Error when attempting to find " + object_name + ": " + err);
    }
    else if (!result) {
      logger.info(object_name + " returned null as expected.")
      not_found_action();
    } else {
      assert.ok(false, "Found " + object_name + " unexpectedly.")
    }    
  };
}

function _aborted_save(column_family, level, object, object_name, next) {
  column_family.add_callback("before_save_" + level, function(previous_version, cb_finished) { 
    cb_finished(new Error("Aborting the save."));
  });
  object.save(function(err, result) {
    if (err) {
      logger.info("Save for " + object_name + " generated error as expected.");
      column_family.callbacks = {}
      next();      
    } else {
      assert.ok(false, "Save for " + object_name + " unexpectedly succeeded.");      
    }
  });    
}

function tokenCallbackManager() {
  var callbackResults = {};
  
  return {
    add: function(column_family, cb_names, token) {
      cb_names.forEach(function(cb_name) {
        callbackResults[cb_name] = [];
        [0, 1].forEach(function(n) {
          var token_name = n;
          var cb;
          if (cb_name.match(/save/) || cb_name.match(/destroy/)) {
            cb = function(previous_version, cb_finished) {
              callbackResults[cb_name].push({name:token_name, token: token});
              cb_finished();
            }
          } else if (cb_name.match(/init/)){
            cb = function() { 
              callbackResults[cb_name].push({name:token_name, token: token});
            }
          } else {
            cb = function(cb_finished) { 
              callbackResults[cb_name].push({name:token_name, token: token});
              cb_finished();
            }            
          }
          column_family.add_callback(cb_name, cb);
        });
      });
    },

    assert: function(cb_names, token) {
      cb_names.forEach(function(cb_name) {
        [0, 1].forEach(function(i) {
          assert.equal(callbackResults[cb_name][i].name, i);
        })
      })
    }    
  }
}
