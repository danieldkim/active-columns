require('./init-test-keyspace').do_it(function(err, keyspaces) {

if (err) throw new Error("Could not initialize keyspace");

var assert = require('assert');
var log4js = require('log4js-node');
log4js.addAppender(log4js.consoleAppender());
var path_nodes = __filename.split('/');
var logger_name = path_nodes[path_nodes.length-1].split('.')[0];
var logger = log4js.getLogger(logger_name);
logger.setLevel('DEBUG');
var sys = require('sys')
var _ = require('underscore')._

var ActiveColumns = require('active-columns');
ActiveColumns.set_logger(logger);
  
var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");
alice = Users1.new_object("alice", {
 city: "New York", state: "NY", last_login: 1271184168, sex: "F"
});
sys.puts(sys.inspect(alice));
  
var Users2 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users2");
var alice_columns = [
 {name: "city", value: "New York"},
 {name: "state", value:"NY"}, 
 {name: "last_login", value: 1271184168}, 
 {name: "sex", value: "F"}
];
alice = Users2.new_object("alice", alice_columns);
sys.puts(sys.inspect(alice));


var StateUsers1 = ActiveColumns.get_column_family("ActiveColumnsTest", "StateUsers1");
var alice_value = {name: "alice", city: "New York", sex: "F" };
var bob_value = {name: "bob", city: "Jackson Heights", sex: "M" };
var ny = StateUsers1.new_object("NY", [
  alice_value,
  bob_value
]);
sys.puts(sys.inspect(ny));

var StateLastLoginUsers = ActiveColumns.get_column_family("ActiveColumnsTest", "StateLastLoginUsers");

var alice_value = {name: "alice", city: "New York", sex: "F" };
var alice_new_city = "Los Angeles"
var ny_1271184168_alice = StateLastLoginUsers.new_object("NY", 1271184168, "alice", alice_value);
sys.puts(sys.inspect(ny_1271184168_alice));

var alice_value = {name: "alice", city: "New York", sex: "F" };
var bob_value = {name: "bob", city: "Jackson Heights", sex: "M" };
var alice_new_city = "Los Angeles"
var bob_new_city = "San Francisco"
var ny_1271184168 = StateLastLoginUsers.new_object("NY", 1271184168, [
  bob_value,
  alice_value
]);
sys.puts(sys.inspect(ny_1271184168));

var alice_value = {name: "alice", city: "New York", sex: "F" };
var bob_value = {name: "bob", city: "Jackson Heights", sex: "M" };
var chuck_value = {name: "chuck", city: "Elmhurst", sex: "M" };
var dave_value = {name: "dave", city: "Brooklyn", sex: "F" };
var ny = StateLastLoginUsers.new_object("NY", [
  {name: 1271184169, columns:[dave_value, chuck_value]},
  {name: 1271184168, columns:[bob_value, alice_value]}
]);
sys.puts(sys.inspect(ny));
sys.puts("JSON: " + JSON.stringify(ny));

});
