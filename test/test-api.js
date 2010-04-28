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

var test_helpers = require('./test-helpers') 
test_helpers.set_logger(logger);

var sync_tests = {
}
for (var test_name in sync_tests) {
  test_helpers.run_sync_test(test_name, sync_tests[test_name]);
}

// setTimeout(function() {
test_helpers.run_async_tests_sequentially([
  ["Test Users1 column family", test_Users1 ],
  ["Test Users2 column family", test_Users2 ],
  ["Test StateUsers1 column family, user level",
    test_StateUsers1_user_level],
  ["Test StateUsers1 column family, state level",
    test_StateUsers1_state_level],
  ["Test StateUsers2 column family, user level", 
    test_StateUsers2_user_level ],
  ["Test StateUsers2 column family, state level", 
    test_StateUsers2_state_level ],
  ["Test StateLastLoginUsers column family, user level", 
    test_StateLastLoginUsers_user_level],
  ["Test StateLastLoginUsers column family, last login level level", 
    test_StateLastLoginUsers_last_login_level],
  ["Test StateLastLoginUsers column family, state level", 
    test_StateLastLoginUsers_state_level],
  ["Test auto key generation", 
    test_auto_key_generation],
  // ["Test auto super column name generation", 
  //   test_auto_super_column_name_generation],
  // ["Test auto column name generation", 
  //   test_auto_column_name_generation]
]);
// }, 30000);

function test_Users1(test_done) {

  var alice, bob;
  function clean_up(clean_up_done) {
    Users1.callbacks = {};
    if (alice && alice.destroy) {
      alice.destroy({
        success: function(result) {
          logger.info("Alice destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying alice." + mess)
        }        
      });
    }
    if (bob && bob.destroy) {
      bob.destroy({
        success: function(result) {
          logger.info("Bob destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying bob." + mess)
        }        
      });
      
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");
  
  var alice_string;
  var bob_string;
  
   // Users1.find("alice", {
   //   success: function(alice) { alice.destroy({success: function() {start();}}); }
   // })
  start();
  
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function(result) {
      logger.info("not_found event for alice generated as expected.")
      if (!finish) unsuccessful_destroy();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    Users1.find("alice", {
      success: function() {
        assert.ok(false, "Found alice unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find alice: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    alice = Users1.new_object("alice", {
     city: "New York", state: "NY", last_login: 1271184168, sex: "F"
    });
    try {
      alice.destroy();
      assert.ok(false, "Expected destroy for alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy  for alice threw exception as expected: " + e);
      first_save();
    }
  }

  var cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(Users1, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      Users1.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert.strictEqual(alice, this);
          event_listeners.success();
        }));      
    });
    alice.save({
     success: function(result) {
       logger.info("Alice saved successfully.");
       assert.equal(cb_token, alice.before_save_token);
       assert.equal(cb_token, alice.after_save_token);
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save alice: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(Users1, ["after_find_row", "after_initialize_row"], cb_token);
    Users1.find("alice", {
      success: clean_up_on_exception_for(function(result) {
        alice = result;
        assert_alice(alice, "New York");
        assert_alice(alice._last_saved, "New York");
        assert.equal(cb_token, alice.after_find_token);
        assert.equal(cb_token, alice.after_initialize_token);
        logger.info("Alice found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: function() {
        assert.ok(false, "Could not find alice.")
      },
      error: function(mess) {
        assert.ok(false, "Error looking for alice:" + mess )
      }
    });
    
  }
  
  function assert_alice(version, city) {
    assert.equal("alice", version.key);
    assert.equal("alice", version.id);
    assert.equal(city, version.city);
    assert.equal("NY", version.state);
    assert.equal(1271184168, version.last_login);
    assert.equal("F", version.sex);       
  }

  function save_after_find() {
    alice.city = "Los Angeles";
    assert_alice(alice, "Los Angeles");
    assert_alice(alice._last_saved, "New York");
    Users1.callbacks = {}
    cb_token = Math.random();;
    add_token_callbacks(Users1, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      Users1.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_alice(previous_version, "New York");
          assert.strictEqual(alice, this);
          event_listeners.success();
        }));      
    });
    alice.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, alice.before_save_token);
        assert.equal(cb_token, alice.after_save_token);
        Users1.find("alice", {
          success: clean_up_on_exception_for(function(result) {
            logger.info("Alice saved successfully after find.");
            alice = result;
            assert.equal("Los Angeles", alice.city);
            Users1.callbacks = {}
            clean_up_on_exception_for(add_bob_to_the_mix)();
          }),
          error: function(mess) {
            assert.ok(false, "Error finding alice.")
          }
        });
      }),
      error: function(mess) {
        assert.ok(false, "Error saving alice: " + mess);        
      }
    });
  }
  
  function add_bob_to_the_mix() {
    bob = Users1.new_object("bob", {
     city: "Jackson Heights", state: "NY", last_login: 1271184168, sex: "M"
    });
    bob.save({
      success: function(result) {
        logger.info("Saved bob successfully.");
        clean_up_on_exception_for(find_alice_and_bob)();
      },
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error saving bob: " + mess);        
      })
    });
  }
  
  function find_alice_and_bob() {
    Users1.find({start_key:'', end_key:'', count: 100}, {
      success: clean_up_on_exception_for(function(results) {
        assert.equal(2, results.length);
        results.forEach(function(res) {
          if (res.key == "alice") {
            alice = res;
            assert_alice(alice, "Los Angeles");
          } else if (res.key == "bob") {
            bob = res;
            assert_bob();
          } else {
            assert.ok(false, "Got an unexpected key from find with key range.")
          }
        })
        clean_up_on_exception_for(successful_destroy)();
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error finding bob and alice: " + mess);        
      })
    })
  }
  
  function assert_bob() {
    assert.equal("bob", bob.key);
    assert.equal("bob", bob.id);
    assert.equal("Jackson Heights", bob.city);
    assert.equal("NY", bob.state);
    assert.equal("1271184168", bob.last_login);
    assert.equal("M", bob.sex);    
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      Users1.add_callback("after_destroy_row", clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(alice, this);
          event_listeners.success();
        }));
      alice.destroy({
        success: function() {
          Users1.callbacks = {}
          assert.equal(cb_token, alice.after_destroy_token);
          logger.info("Successfully destroyed alice.")
          clean_up_on_exception_for(function(){unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying alice: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for alice threw an exception.");
    }    
  }
    
}

function test_Users2(test_done) {

  var alice, bob;
  function clean_up(clean_up_done) {
    if (alice && alice.destroy) {
      alice.destroy({
        success: function(result) {
          logger.info("Alice destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying alice." + mess)
        }        
      });
    }
    if (bob && bob.destroy) {
      bob.destroy({
        success: function(result) {
          logger.info("Bob destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying bob." + mess)
        }        
      });
      
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var Users2 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users2");
  
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
  
   // Users2.find("alice", {column_names:["city", "state", "last_login", "sex"]}, {
   //    success: function(alice) { alice.destroy({success: function() {start();}}); }
   // })
  start();
    
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function() {
      logger.info("not_found event for alice generated as expected.")
      if (!finish) clean_up_on_exception_for(unsuccessful_destroy)();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    Users2.find("alice", predicate, {
      success: function() {
        assert.ok(false, "Found alice unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find alice: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    alice = Users2.new_object("alice", alice_columns);
    try {
      alice.destroy();
      assert.ok(false, "Expected destroy for alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for alice threw exception as expected: " + e);
      first_save();
    }
  }
  
  var cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(Users2, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      Users2.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert.strictEqual(alice, this);
          event_listeners.success();
        }));      
    });
    alice.save({
     success: function(result) {
       logger.info("Alice saved successfully.");
       assert.equal(cb_token, alice.before_save_token);
       assert.equal(cb_token, alice.after_save_token);
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save alice: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(Users2, ["after_find_row", "after_initialize_row"], cb_token);
    Users2.find("alice", predicate, {
      success: clean_up_on_exception_for(function(result) {
        alice = result;
        assert_alice(alice, "New York");
        assert.equal(cb_token, alice.after_find_token);
        assert.equal(cb_token, alice.after_initialize_token);
        logger.info("Alice found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: function() {
        assert.ok(false, "Could not find alice.")
      },
      error: function(mess) {
        assert.ok(false, "Error looking for alice:" + mess )
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
    add_token_callbacks(Users2, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      Users2.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_alice(previous_version, "New York");
          assert.strictEqual(alice, this);
          event_listeners.success();
        }));      
    });
    alice.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, alice.before_save_token);
        assert.equal(cb_token, alice.after_save_token);
        logger.info("Alice saved successfully after find.");
        Users2.find("alice", predicate, {
          success: clean_up_on_exception_for(function(result) {
            alice = result;
            assert_alice(alice, "Los Angeles");
            logger.info("Save after find validated.");
            Users2.callbacks = {};
            clean_up_on_exception_for(add_bob_to_the_mix)();
          }),
          error: clean_up_on_exception_for(function(mess) {
            assert.ok(false, "Error finding alice.")
          })
        });
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error saving alice: " + mess);        
      })
    });
  }
   
  function add_bob_to_the_mix() {
    bob = Users2.new_object("bob", bob_columns);
    bob.save({
      success: function(result) {
        logger.info("Saved bob successfully.");
        clean_up_on_exception_for(find_alice_and_bob)();
      },
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error saving bob: " + mess);        
      })
    });
  }
  
  function find_alice_and_bob() {
    Users2.find({start_key:'', end_key:'', count: 100}, predicate, {
      success: clean_up_on_exception_for(function(results) {
        assert.equal(2, results.length);
        results.forEach(function(res) {
          if (res.key == "alice") {
            alice = res;
            assert_alice(alice, "Los Angeles");
          } else if (res.key == "bob") {
            bob = res;
            assert_bob();
          } else {
            assert.ok(false, "Got an unexpected key from find with key range.")
          }
        })
        clean_up_on_exception_for(successful_destroy)();
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error finding bob and alice: " + mess);        
      })
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
      Users2.add_callback("after_destroy_row", clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(alice, this);
          event_listeners.success();
        }));
      alice.destroy({
        success: function() {
          logger.info("Successfully destroyed alice.")
          assert.equal(cb_token, alice.after_destroy_token);
          Users2.callbacks = {}
          clean_up_on_exception_for(function() {unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying alice: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for alice threw an exception.");
    }    
  }
    
}

function test_StateUsers1_user_level(test_done) {
  _test_StateUsersX_user_level(test_done, "StateUsers1")
}

function test_StateUsers1_state_level(test_done) {
  _test_StateUsersX_state_level(test_done, "StateUsers1")
}

function test_StateUsers2_user_level(test_done) {
  _test_StateUsersX_user_level(test_done, "StateUsers2")
}

function test_StateUsers2_state_level(test_done) {
  _test_StateUsersX_state_level(test_done, "StateUsers2")
}

function _test_StateUsersX_user_level(test_done, column_family) {

  var ny_alice;
  function clean_up(clean_up_done) {
    StateUsersX.callbacks = {}
    if (ny_alice && ny_alice.destroy) {
      ny_alice.destroy({
        success: function(result) {
          logger.info("ny_alice destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying ny_alice." + mess)
        }        
      });
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var StateUsersX = ActiveColumns.get_column_family("ActiveColumnsTest", column_family);
  var callback_level = column_family == "StateUsers1" ? "super_column" : "column"
  
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var alice_new_city = "Los Angeles"
  
   // StateUsersX.find("NY", "alice", {
   //   success: function(ny_alice) {
   //     ny_alice.destroy({success: function() {start();}}); 
   //   }
   // })
  start();
  
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function(result) {
      logger.info("not_found event for ny generated as expected.")
      if (!finish) unsuccessful_destroy();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    StateUsersX.find("NY", "alice", {
      success: function() {
        assert.ok(false, "Found ny_alice unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find ny_alice: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    ny_alice = StateUsersX.new_object("NY", "alice", alice_value);
    try {
      ny_alice.destroy();
      assert.ok(false, "Expected destroy for ny_alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny_alice threw exception as expected: " + e);
      clean_up_on_exception_for(first_save)();
    }
  }
  
  var cb_names = ["before_save_" + callback_level, "after_save_" + callback_level];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(StateUsersX, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert_ny_alice(ny_alice, "New York");
          event_listeners.success();
        }));      
    });
    ny_alice.save({
     success: function(result) {
       logger.info("ny_alice saved successfully.");
       assert.equal(cb_token, ny_alice.before_save_token);
       assert.equal(cb_token, ny_alice.after_save_token);
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save ny_alice: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(StateUsersX, ["after_find_" + callback_level, "after_initialize_" + callback_level], cb_token);
    StateUsersX.find("NY", "alice", {
      success: clean_up_on_exception_for(function(result) {
        ny_alice = result;
        assert_ny_alice(ny_alice, "New York");
        assert.equal(cb_token, ny_alice.after_find_token);
        assert.equal(cb_token, ny_alice.after_initialize_token);
        logger.info("ny_alice found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: clean_up_on_exception_for(function() {
        assert.ok(false, "Could not find ny_alice.")
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error looking for ny_alice:" + mess )
      })
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
    add_token_callbacks(StateUsersX, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_ny_alice(previous_version, prev_city);
          assert.strictEqual(ny_alice, this);
          event_listeners.success();
        }));      
    });
    ny_alice.city = alice_new_city
    ny_alice.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, ny_alice.before_save_token);
        assert.equal(cb_token, ny_alice.after_save_token);
        StateUsersX.find("NY", "alice", {
          success: clean_up_on_exception_for(function(result) {
            ny_alice = result;
            assert_ny_alice(ny_alice, alice_new_city);
            logger.info("ny_alice saved successfully and result validated after find.");
            clean_up_on_exception_for(successful_destroy)();
          }),
          error: function(mess) {
            assert.ok(false, "Error finding ny_alice.")
          }
        });
      }),
      error: function(mess) {
        assert.ok(false, "Error saving ny_alice: " + mess);        
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateUsersX.add_callback("after_destroy_" + callback_level, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(ny_alice, this);
          event_listeners.success();
        }));
      ny_alice.destroy({
        success: function() {
          StateUsersX.callbacks = {}
          assert.equal(cb_token, ny_alice.after_destroy_token);
          logger.info("Successfully destroyed ny_alice.")
          clean_up_on_exception_for(function(){unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying ny_alice: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny_alice threw an exception.");
    }    
  }
    
}

function _test_StateUsersX_state_level(test_done, column_family) {

  var ny, ny_alice, ny_bob;
  function clean_up(clean_up_done) {
    StateUsersX.callbacks = {}
    if (ny && ny.destroy) {
      ny.destroy({
        success: function(result) {
          logger.info("ny destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying ny." + mess)
        }        
      });
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var StateUsersX = ActiveColumns.get_column_family("ActiveColumnsTest", column_family);
  
  var column_predicate = {slice_range:{start:'a', finish: 'c', reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var bob_value = {_name: "bob", city: "Jackson Heights", sex: "M" };
  var alice_new_city = "Los Angeles"
  var bob_new_city = "San Francisco"
  
   // StateUsersX.find("NY", column_predicate, {
   //   success: function(ny) {
   //     ny.destroy({success: function() {start();}}); 
   //   }
   // })
  start();
  
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function(result) {
      logger.info("not_found event for ny generated as expected.")
      if (!finish) unsuccessful_destroy();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    StateUsersX.find("NY", column_predicate, {
      success: function() {
        assert.ok(false, "Found ny unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find ny: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    ny = StateUsersX.new_object("NY", [
      alice_value,
      bob_value
    ]);
    try {
      ny.destroy();
      assert.ok(false, "Expected destroy for ny to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny threw exception as expected: " + e);
      clean_up_on_exception_for(first_save)();
    }
  }
  
  var cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(StateUsersX, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert.strictEqual(ny, this);
          event_listeners.success();
        }));      
    });
    ny.save({
     success: function(result) {
       logger.info("ny saved successfully.");
       assert.equal(cb_token, ny.before_save_token);
       assert.equal(cb_token, ny.after_save_token);
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save ny: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(StateUsersX, ["after_find_row", "after_initialize_row"], cb_token);
    StateUsersX.find("NY", column_predicate, {
      success: clean_up_on_exception_for(function(result) {
        ny = result;
        assert.equal(2, ny.columns.length);
        assert.equal(cb_token, ny.after_find_token);
        assert.equal(cb_token, ny.after_initialize_token);
        ny_alice = ny.columns[0];
        ny_bob = ny.columns[1];
        assert_ny_alice(ny_alice, "New York");
        assert_ny_bob(ny_bob, "Jackson Heights");
        logger.info("ny found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: clean_up_on_exception_for(function() {
        assert.ok(false, "Could not find ny.")
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error looking for ny:" + mess )
      })
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
    add_token_callbacks(StateUsersX, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateUsersX.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_ny_alice(previous_version.columns[0], prev_city);
          assert.strictEqual(ny, this);
          event_listeners.success();
        }));      
    });
    ny.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, ny.before_save_token);
        assert.equal(cb_token, ny.after_save_token);
        StateUsersX.find("NY", column_predicate, {
          success: clean_up_on_exception_for(function(result) {
            ny = result;
            ny_alice = result.columns[0];
            ny_bob = result.columns[1];
            assert_ny_alice(ny_alice, alice_new_city);
            assert_ny_bob(ny_bob, bob_new_city);
            logger.info("ny saved successfully and result validated after find.");
            clean_up_on_exception_for(successful_destroy)();
          }),
          error: function(mess) {
            assert.ok(false, "Error finding ny.")
          }
        });
      }),
      error: function(mess) {
        assert.ok(false, "Error saving ny: " + mess);        
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateUsersX.add_callback("after_destroy_row", clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(ny, this);
          event_listeners.success();
        }));
      ny.destroy({
        success: function() {
          logger.info("Successfully destroyed ny.")
          StateUsersX.callbacks = {}
          assert.equal(cb_token, ny.after_destroy_token);
          clean_up_on_exception_for(function(){unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying ny: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny threw an exception.");
    }    
  }
    
}
    
function test_StateLastLoginUsers_user_level(test_done) {

  var ny_1271184168_alice;
  function clean_up(clean_up_done) {
    StateLastLoginUsers.callbacks = {}
    if (ny_1271184168_alice && ny_1271184168_alice.destroy) {
      ny_1271184168_alice.destroy({
        success: function(result) {
          logger.info("ny_1271184168_alice destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying ny_1271184168_alice." + mess)
        }        
      });
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
  
  var column_predicate = {slice_range:{start:'', finish: '', reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var alice_new_city = "Los Angeles"
  
   // StateLastLoginUsers.find("NY", 1271184168, "alice", {
   //   success: clean_up_on_exception_for(function(ny_1271184168_alice) {
   //     ny_1271184168_alice.destroy({
   //       success: function() {
   //         start();
   //       },
   //       error: function(mess) {
   //         assert.ok(false, "Error destroying ny_1271184168_alice: " + mess)
   //       }
   //     }); 
   //   })
   // })
  start();
  
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function(result) {
      logger.info("not_found event for ny_1271184168_alice generated as expected.")
      if (!finish) unsuccessful_destroy();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    StateLastLoginUsers.find("NY", 1271184168, "alice", {
      success: function() {
        assert.ok(false, "Found ny_1271184168_alice unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find ny_1271184168_alice: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    ny_1271184168_alice = StateLastLoginUsers.new_object("NY", 1271184168, "alice", alice_value);
    try {
      ny_1271184168_alice.destroy();
      assert.ok(false, "Expected destroy for ny_1271184168_alice to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny_1271184168_alice threw exception as expected: " + e);
      clean_up_on_exception_for(first_save)();
    }
  }
  
  var cb_names = ["before_save_column", "after_save_column"];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(StateLastLoginUsers, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert_ny_1271184168_alice(this, "New York");
          event_listeners.success();
        }));      
    });
    ny_1271184168_alice.save({
     success: function(result) {
       logger.info("ny_1271184168_alice saved successfully.");
       assert.equal(cb_token, ny_1271184168_alice.before_save_token);
       assert.equal(cb_token, ny_1271184168_alice.after_save_token);
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save ny_1271184168_alice: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(StateLastLoginUsers, ["after_find_column", "after_initialize_column"], cb_token);
    StateLastLoginUsers.find("NY", 1271184168, "alice", {
      success: clean_up_on_exception_for(function(result) {
        ny_1271184168_alice = result;
        assert_ny_1271184168_alice(ny_1271184168_alice, "New York");
        assert.equal(cb_token, ny_1271184168_alice.after_find_token);
        assert.equal(cb_token, ny_1271184168_alice.after_initialize_token);
        logger.info("ny_1271184168_alice found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: clean_up_on_exception_for(function() {
        assert.ok(false, "Could not find ny_1271184168_alice.")
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error looking for ny_1271184168_alice:" + mess )
      })
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
    add_token_callbacks(StateLastLoginUsers, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_ny_1271184168_alice(previous_version, prev_city);
          assert_ny_1271184168_alice(this, alice_new_city);
          event_listeners.success();
        }));      
    });
    ny_1271184168_alice.city = alice_new_city
    ny_1271184168_alice.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, ny_1271184168_alice.before_save_token);
        assert.equal(cb_token, ny_1271184168_alice.after_save_token);
        StateLastLoginUsers.find("NY", 1271184168, "alice", {
          success: clean_up_on_exception_for(function(result) {
            ny_1271184168_alice = result;
            assert_ny_1271184168_alice(ny_1271184168_alice, alice_new_city);
            logger.info("ny_1271184168_alice saved successfully and result validated after find.");
            clean_up_on_exception_for(successful_destroy)();
          }),
          error: function(mess) {
            assert.ok(false, "Error finding ny_1271184168_alice.")
          }
        });
      }),
      error: function(mess) {
        assert.ok(false, "Error saving ny_1271184168_alice: " + mess);        
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateLastLoginUsers.add_callback("after_destroy_column", clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(ny_1271184168_alice, this);
          event_listeners.success();
        }));
      ny_1271184168_alice.destroy({
        success: function() {
          logger.info("Successfully destroyed ny_1271184168_alice.")
          StateLastLoginUsers.callbacks = {}
          assert.equal(cb_token, ny_1271184168_alice.after_destroy_token);
          clean_up_on_exception_for(function(){unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying ny_1271184168_alice: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny_1271184168_alice threw an exception.");
    }    
  }
    
}

function test_StateLastLoginUsers_last_login_level(test_done) {

  var ny_1271184168;
  function clean_up(clean_up_done) {
    StateLastLoginUsers.callbacks = {}
    if (ny_1271184168 && ny_1271184168.destroy) {
      ny_1271184168.destroy({
        success: function(result) {
          logger.info("ny_1271184168 destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying ny_1271184168." + mess)
        }        
      });
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
  
  var column_predicate = {slice_range:{start:'', finish: '', reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var bob_value = {_name: "bob", city: "Jackson Heights", sex: "M" };
  var alice_new_city = "Los Angeles"
  var bob_new_city = "San Francisco"
  
   // StateLastLoginUsers.find("NY", 1271184168, column_predicate, {
   //   success: function(ny_1271184168) {
   //     ny_1271184168.destroy({success: function() {start();}}); 
   //   }
   // })
  start();
  
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function(result) {
      logger.info("not_found event for ny generated as expected.")
      if (!finish) unsuccessful_destroy();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    StateLastLoginUsers.find("NY", 1271184168, column_predicate, {
      success: function() {
        assert.ok(false, "Found ny_1271184168 unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find ny_1271184168: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    ny_1271184168 = StateLastLoginUsers.new_object("NY", 1271184168, [
      bob_value,
      alice_value
    ]);
    try {
      ny_1271184168.destroy();
      assert.ok(false, "Expected destroy for ny_1271184168 to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny_1271184168 threw exception as expected: " + e);
      clean_up_on_exception_for(first_save)();
    }
  }
  
  var cb_names = ["before_save_super_column", "after_save_super_column"];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(StateLastLoginUsers, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert_ny_1271184168(this, 1, "New York", 0, "Jackson Heights");
          event_listeners.success();
        }));      
    });
    ny_1271184168.save({
     success: function(result) {
       assert.equal(cb_token, ny_1271184168.before_save_token);
       assert.equal(cb_token, ny_1271184168.after_save_token);
       logger.info("ny_1271184168 saved successfully.");
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save ny_1271184168: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(StateLastLoginUsers, ["after_find_super_column", "after_initialize_super_column"], cb_token);
    StateLastLoginUsers.find("NY", 1271184168, column_predicate, {
      success: clean_up_on_exception_for(function(result) {
        ny_1271184168 = result;
        assert_ny_1271184168(ny_1271184168, 0, "New York", 1, "Jackson Heights");
        assert.equal(cb_token, ny_1271184168.after_find_token);
        assert.equal(cb_token, ny_1271184168.after_initialize_token);
        logger.info("ny_1271184168 found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: clean_up_on_exception_for(function() {
        assert.ok(false, "Could not find ny_1271184168.")
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error looking for ny_1271184168:" + mess )
      })
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
    add_token_callbacks(StateLastLoginUsers, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_ny_1271184168(previous_version, 0, alice_prev_city, 1, bob_prev_city);
          assert_ny_1271184168(this, 0, alice_new_city, 1, bob_new_city);
          event_listeners.success();
        }));      
    });
    ny_1271184168.columns[0].city = alice_new_city
    ny_1271184168.columns[1].city = bob_new_city
    ny_1271184168.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, ny_1271184168.before_save_token);
        assert.equal(cb_token, ny_1271184168.after_save_token);
        StateLastLoginUsers.find("NY", 1271184168, column_predicate, {
          success: clean_up_on_exception_for(function(result) {
            ny_1271184168 = result;
            assert_ny_1271184168(ny_1271184168, 0, alice_new_city, 1, bob_new_city);
            logger.info("ny_1271184168 saved successfully and result validated after find.");
            clean_up_on_exception_for(successful_destroy)();
          }),
          error: function(mess) {
            assert.ok(false, "Error finding ny_1271184168.")
          }
        });
      }),
      error: function(mess) {
        assert.ok(false, "Error saving ny_1271184168: " + mess);        
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateLastLoginUsers.add_callback("after_destroy_super_column", clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(ny_1271184168, this);
          event_listeners.success();
        }));
      ny_1271184168.destroy({
        success: function() {
          logger.info("Successfully destroyed ny_1271184168.")
          StateLastLoginUsers.callbacks = {}
          assert.equal(cb_token, ny_1271184168.after_destroy_token);
          clean_up_on_exception_for(function(){unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying ny_1271184168: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny_1271184168 threw an exception.");
    }    
  }
    
}

function test_StateLastLoginUsers_state_level(test_done) {

  var ny, ny_1271184168, ny_1271184169;
  function clean_up(clean_up_done) {
    StateLastLoginUsers.callbacks = {}
    if (ny && ny.destroy) {
      ny.destroy({
        success: function(result) {
          logger.info("ny destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying ny." + mess)
        }        
      });
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");
  
  var column_predicate = {slice_range:{start:1271184168, finish: 1271184169, reversed:false, count: 100}}
  var alice_value = {_name: "alice", city: "New York", sex: "F" };
  var bob_value = {_name: "bob", city: "Jackson Heights", sex: "M" };
  var alice_new_city = "Los Angeles"
  var bob_new_city = "San Francisco"
  var chuck_value = {_name: "chuck", city: "Elmhurst", sex: "M" };
  var dave_value = {_name: "dave", city: "Brooklyn", sex: "F" };
  var chuck_new_city = "Seattle"
  var dave_new_city = "Portland"
  
   // StateLastLoginUsers.find("NY", column_predicate, {
   //   success: function(ny) {
   //     ny.destroy({success: function() {start();}}); 
   //   }
   // })
  start();
  
  function start() { unsuccessful_find(); };
  
  function unsuccessful_find(finish) {
    var not_found_func = function(result) {
      logger.info("not_found event for ny generated as expected.")
      if (!finish) unsuccessful_destroy();
    }
    if (finish) {
      not_found_func = clean_up_after(not_found_func);
    }
    StateLastLoginUsers.find("NY", column_predicate, {
      success: function() {
        assert.ok(false, "Found ny unexpectedly.")
      },
      not_found: not_found_func,
      error: function(mess) {
        assert.ok(false, "Error when attempting to find ny: " + mess)
      }    
    })    
  }
  
  function unsuccessful_destroy() {
    ny = StateLastLoginUsers.new_object("NY", [
      {_name: 1271184169, columns:[dave_value, chuck_value]},
      {_name: 1271184168, columns:[bob_value, alice_value]}
    ]);
    try {
      ny.destroy();
      assert.ok(false, "Expected destroy for ny to throw an exception.");
    } catch (e) {
      logger.info("Destroy for ny threw exception as expected: " + e);
      clean_up_on_exception_for(first_save)();
    }
  }
  
  var cb_names = ["before_save_row", "after_save_row"];
  
  function first_save() {
    var cb_token = Math.random();
    add_token_callbacks(StateLastLoginUsers, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert.equal(null, previous_version);
          assert.strictEqual(ny, this);
          event_listeners.success();
        }));      
    });
    ny.save({
     success: function(result) {
       assert.equal(cb_token, ny.before_save_token);
       assert.equal(cb_token, ny.after_save_token);
       logger.info("ny saved successfully.");
       clean_up_on_exception_for(successful_find)();
     },
     error: function(mess) {
       assert.ok(false, "Error trying to save ny: " + mess)
     }
    });    
  }
  
  function successful_find() {
    var cb_token = Math.random();
    add_token_callbacks(StateLastLoginUsers, ["after_find_row", "after_initialize_row"], cb_token);
    StateLastLoginUsers.find("NY", column_predicate, {
      success: clean_up_on_exception_for(function(result) {
        ny = result;
        assert.equal(2, ny.columns.length);
        assert.equal(cb_token, ny.after_find_token);
        assert.equal(cb_token, ny.after_initialize_token);
        ny_1271184168 = ny.columns[0];
        ny_1271184169 = ny.columns[1];
        assert_ny_1271184168(ny_1271184168, 0, "New York", 1, "Jackson Heights");
        assert_ny_1271184169(ny_1271184169, 0, "Elmhurst", 1, "Brooklyn");
        logger.info("ny found successfully and save result validated.");
        clean_up_on_exception_for(save_after_find)();
      }),
      not_found: clean_up_on_exception_for(function() {
        assert.ok(false, "Could not find ny.")
      }),
      error: clean_up_on_exception_for(function(mess) {
        assert.ok(false, "Error looking for ny:" + mess )
      })
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
    add_token_callbacks(StateLastLoginUsers, cb_names, cb_token);
    cb_names.forEach(function(cb_name) {
      StateLastLoginUsers.add_callback(cb_name, clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          assert_ny_1271184168(previous_version.columns[0], 0, alice_prev_city, 1, bob_prev_city);
          assert_ny_1271184169(previous_version.columns[1], 0, chuck_prev_city, 1, dave_prev_city);
          assert.strictEqual(ny, this);
          event_listeners.success();
        }));      
    });
    ny.columns[0].columns[0].city = alice_new_city
    ny.columns[0].columns[1].city = bob_new_city
    ny.columns[1].columns[0].city = chuck_new_city
    ny.columns[1].columns[1].city = dave_new_city
    ny.save({
      success: clean_up_on_exception_for(function(result) {
        assert.equal(cb_token, ny.before_save_token);
        assert.equal(cb_token, ny.after_save_token);
        StateLastLoginUsers.find("NY", column_predicate, {
          success: clean_up_on_exception_for(function(result) {
            ny = result;
            ny_1271184168 = result.columns[0];
            ny_1271184169 = result.columns[1];
            assert_ny_1271184168(ny_1271184168, 0, alice_new_city, 1, bob_new_city);
            assert_ny_1271184169(ny_1271184169, 0, chuck_new_city, 1, dave_new_city);
            logger.info("ny saved successfully and result validated after find.");
            clean_up_on_exception_for(successful_destroy)();
          }),
          error: function(mess) {
            assert.ok(false, "Error finding ny.")
          }
        });
      }),
      error: function(mess) {
        assert.ok(false, "Error saving ny: " + mess);        
      }
    });
  }
  
  function successful_destroy() {
    try {
      var cb_token = Math.random();;
      StateLastLoginUsers.add_callback("after_destroy_row", clean_up_on_exception_for(
        function(event_listeners, previous_version) {
          this.after_destroy_token = cb_token;
          assert.strictEqual(ny, this);
          event_listeners.success();
        }));
      ny.destroy({
        success: function() {
          logger.info("Successfully destroyed ny.")
          StateLastLoginUsers.callbacks = {}
          assert.equal(cb_token, ny.after_destroy_token);
          clean_up_on_exception_for(function(){unsuccessful_find(true)})();
        },
        error: function(mess) {          
          assert.ok(false, "Error destroying ny: " + mess)
        }
      });
    } catch (e) {
      assert.ok(false, "Destroy for ny threw an exception.");
    }    
  }
    
}

function test_auto_key_generation(test_done) {
  var alice, bob;
  function clean_up(clean_up_done) {
    if (alice && alice.destroy) {
      alice.destroy({
        success: function(result) {
          logger.info("Alice destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying alice." + mess)
        }        
      });
    }
    if (bob && bob.destroy) {
      bob.destroy({
        success: function(result) {
          logger.info("Bob destroyed in clean_up.")
          clean_up_done();
        },
        error: function(mess) {
          assert.ok(false, "error destroying bob." + mess)
        }        
      });
    }
  }
  var clean_up_wrapper = test_helpers.clean_up_wrapper_factory(clean_up, test_done)
  var clean_up_after = clean_up_wrapper.clean_up_after
  var clean_up_on_exception_for = clean_up_wrapper.clean_up_on_exception_for
  var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");
  var uuid_regex = /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$/;
  
  alice = Users1.new_object({
   city: "New York", state: "NY", last_login: 1271184168, sex: "F"
  });
  alice.save({
   success: clean_up_on_exception_for(function(result) {
     assert.ok(result.match(uuid_regex),
               "Save for alice failed to return a UUID.");
     assert.equal(result, alice.key,
                  "Save for alice failed to set key property.");
     logger.info("Save for alice successfully returned a UUID.")
     clean_up_on_exception_for(do_bob_now)();
   }),
   error: clean_up_on_exception_for(function(mess) {
     assert.ok(false, "Error trying to save alice: " + mess)
   })
  });
  
  function do_bob_now() {
    bob = Users1.new_object(null, {
     city: "Jackson Heights", state: "NY", last_login: 1271184169, sex: "M"
    });
    bob.save({
     success: clean_up_after(function(result) {
       assert.ok(result.match(uuid_regex),
                 "Save for bob failed to return a UUID.");
       assert.equal(result, bob.key,
                    "Save for bob failed to set key property.");
       logger.info("Save for bob successfully returned a UUID.")
     }),
     error: clean_up_on_exception_for(function(mess) {
       sys.puts("--- bob got an error :(")
       assert.ok(false, "Error trying to save bob: " + mess)
     })
    });    
  }
}

function add_token_callbacks(column_family, cb_names, token) {
  cb_names.forEach(function(cb_name) {
    column_family.add_callback(cb_name, function(event_listeners) { 
      this[cb_name.match(/^(\w+?_\w+?)_/)[1] + "_token"] = token;
      event_listeners.success();
    });      
  })
}