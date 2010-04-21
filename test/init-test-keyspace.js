var ActiveColumns = require('active-columns');

exports.do_it = function() {
  ActiveColumns.initialize_keyspaces([
    { 
      name: "ActiveColumnsTest", 
      cassandra_port: 10000,
      cassandra_host: "127.0.0.1", 
      column_families: {
        Users1: { 
          column_names: ["city", "state", "last_login", "sex"],
        },
        Users2: {},
        StateUsers1: { type: "Super", subcolumn_names:  ["city", "sex"] },
        StateUsers2: { column_value_type: "json" },
        StateLastLoginUsers: { type: "Super", subcolumn_value_type: "json" }
      }
    }
  ]);
  
};