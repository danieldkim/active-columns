var ActiveColumns = require('active-columns');

exports.do_it = function() {
  ActiveColumns.initialize_keyspaces({ 
    ActiveColumnsTest: {
      cassandra_port: 10000,
      cassandra_host: "127.0.0.1", 
      column_families: {
        Users1: { 
          column_names: ["city", "state", "last_login", "sex"],
        },
        Users2: {},
        StateUsers1: { type: "Super", subcolumn_names:  ["city", "sex"] },
        StateUsers2: { column_value_type: "json" },
        StateLastLoginUsers: { type: "Super", subcolumn_value_type: "json" },
        ColumnValueTypeTest: {
          column_value_types: {date_col: "date", number_col: "number", json_col: "json"}
        },
        ColumnValueTypeTestStatic: {
          column_names: ["date_col", "number_col", "json_col"],
          column_value_types: {date_col: "date", number_col: "number", json_col: "json"}
        }
      }
    }
  });
  
};