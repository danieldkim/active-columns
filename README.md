# Active Columns

Active Columns provides a high-level object-oriented Javascript interface to
Cassandra. Rather than deal with low-level Cassandra API's like
<code>get\_slice()</code> and <code>batch\_mutate()</code>, and raw column
structures and timestamps, you work with *objects* and <code>find()</code>,
<code>save()</code>, and <code>destroy()</code> them.

Active Columns is an asynchronous Javascript framework built on [Cassandra-Node
Bridge](http://github.com/danieldkim/cassandra-node-bridge) and
[Node.js](http://nodejs.org/).

## Requirements

* Node.js >0.1.91
* [Cassandra-Node Bridge](http://github.com/danieldkim/cassandra-node-bridge)
* [Underscore.js](http://documentcloud.github.com/underscore/) 1.0.2

## Object Model

An object is a set of columns under a key or super column. There are 3 types of
objects in Active Columns, organized in a hierarchy with the low-level column
"primitives" that correspond to the Column structures of the Cassandra API:

* Row objects
    * Super column objects
        * Column objects
        * columns
    * Column objects
    * columns


Row objects correspond to the row level in Cassandra and are defined by a key
and column predicate. Super column objects correspond to the super column level
in Cassandra and are defined by a key, a super column name, and a column
predicate.

Column objects correspond to the column level and are defined by a key, an
optional super column name, and a column name -- notice that column objects live
at the same level as column primitives. The internal structure of a column
object is transparent to Cassandra as it is serialized as JSON within it.

Row objects contain super column objects, column objects, or column primitives.
Super column objects contain column objects or column primitives.

## Static and dynamic column names

Active Columns attempts to "collapse" the aggregation structure of objects
whenever possible to make them easier to work with. One tool it has for doing
this is static/fixed column names. Fixed column names can serve as the predicate
that defines all of the objects you work with from a Column family. When column
names are fixed, Active Columns can pull the column primitives up into their
parent as key value pairs. There is also no need to explicitly define the column
predicate when doing a <code>find()</code>.

If every row in a column family or super column will have the same set of fixed
column names, e.g. a "Users" column family with "first\_name", "last_name",
etc., you can configure these column names in Active Columns to make it collapse
User objects, allowing you to access the first name of a user object like so:

    var first_name = my_user.first_name;

instead of like so (this example uses Underscore.js to detect the "first\_name"
column):

    var first_name = _.detect(my_user.columns, function(col) {
      return col.name == "first_name";
    }).value;

The timestamps are still accessible via the timestamps attribute:

    var first_name_timestamp = my_user.timestamps.first_name;

Active Columns uses these timestamps internally when saving/destroying objects,
but you generally shouldn't have to mess with them.

### Numbers as static column names

Not supported.

### _names

Super column and column objects are identified by name. To avoid potential
collisions with attributes named "name", the attribute denoting the name of a
super column or column object is called **"_name"**. Names of column
"primitives" are denoted by "name" as the only possible attributes of a column
primitive are "name", "value", and "timestamp".

## Object model examples

Here are various examples of different object models. The examples here form the
basis of the tests in test/test-api.js. Check them out for a more in-depth look
at how to work with them.

Note that every object in these examples also has an <code>id</code> property,
and <code>save()</code>,<code>destroy()</code>, etc. methods, but they are
elided here.

### "Users1" column family

Example of a row object, under the key "alice":

    {
      key: "alice",
      city: "New York",
      state: "NY",
      last_login: 1271184168,
      timestamps: {
        city: 1270586369995573,
        state: 1270586369995573,
        last_login: 1270586369995573
      }
    }  

This is the most basic type of object with fixed column names that you can work
with in Active Columns. Programmatically we could work with it like so -- create
a new object and save it:

    var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1")
    var alice = Users1.new_object("alice", {
      city: "New York", state: "NY", last_login: 1271184168, sex: 'F'
    });
    alice.save({
      "success" : function(result) {
        sys.puts("Alice saved.");
      },
      "error": function(mess) {
        sys.puts("Error saving alice: " + mess);        
      }
    });

... and then at some point later after a successful save, find it, update it,
and save it again:

    Users1.find("alice", {
      "success": function(alice) {
        alice.city = "Los Angeles";
        alice.save({
          "success" : function() {
            sys.puts("Alice saved.  City is now: " + alice.city);
          },
          "error": function(mess) {
            sys.puts("Error saving alice: " + mess);        
          }
        });
      },
      "not_found": function() {
        sys.puts("Could not find alice.")
      }
      "error": function(mess) {
        sys.puts("Error looking for alice:" + mess )
      }
    });

Normally, when you save an object with fixed column names, any missing columns
will be deleted. If you want to prevent this, you can pass <code>false</code> as
the last argument to <code>save()</code>, after the event listeners hash. This
is useful when you only want to update a small number of columns.

Alternatively, if fixed column names are not specified, Active Columns will
represent the data in this form:

### "Users2" column family

Example of a row object, under the key "alice":

    {
      key: "alice",
      columns: [
        {name: "city", value: "New York", timestamp: 1270586369995573},
        {name: "state", value: "NY", timestamp: 1270586369995573},
        {name: "last_login", value: 1271184168, timestamp: 1270586369995573},
        {name: "sex", value: 'F', timestamp: 1270586369995573}
      ]
    }  

This form is a bit lower level and matches the way cassandra represents it more
closely. Working with it is a bit more verbose:

    var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1")
    var alice = Users1.new_object("alice", [
      {name: "city", value: "New York"},
      {name: "state", value: "NY"},
      {name: "last_login", value: 1271184168},
      {name: "sex", value: 'F'}
    ]);
    alice.save({
      "success" : function(result) {
        sys.puts("Alice saved.");
      },
      "error": function(mess) {
        sys.puts("Error saving alice: " + mess);        
      }
    });

    ...
    
    var predicate = {
      column_names: ["city", "state", "last_login", "sex"]
    }
    Users1.find("alice", predicate, {
      "success": function(alice) {
        _.detect(my_user.columns, function(col) {
          return col.name == "first_name";
        }).value = "Los Angeles";
        alice.save({
          "success" : function() {
            var city = _.detect(my_user.columns, function(col) {
              return col.name == "first_name";
            }).value;
            sys.puts("Alice saved.  City is now: " + city);
          },
          "error": function(mess) {
            sys.puts("Error saving alice: " + mess);        
          }
        });
      },
      "not_found": function() {
        sys.puts("Could not find alice.")
      }
      "error": function(mess) {
        sys.puts("Error looking for alice:" + mess )
      }
    });

Both of these representations of a User are stored in Cassandra in exactly the
same way.

### "StateUsers1" column family

Fixed column names can also be specified at the subcolumn level, i.e. under a
super column. **StateUsers1** indexes users by state, and has fixed subcolumn
names.

Example row object under the key "NY":

    {
      key: "NY",
      columns: [
        { 
          _name: "alice", 
          city: "New York",
          sex: 'F',
          timestamps: {
            name: 1270586369995573,
            city: 1270586369995573,
            sex: 1270586369995573
          }
        },
        { 
          _name: "bob", 
          city: "Jackson Heights",
          sex: 'M',
          timestamps: {
            name: 1270586369995574,
            city: 1270586369995574,
            sex: 1270586369995574
          }
        }
      ] 
    }

### "StateUsers2" column family

Instead of using super columns in the example above, we can use regular columns
with value type json. The object structure we get back from Active Columns is
very similar:

    {
      key: "NY",
      columns: [
        { 
          _name: "alice", 
          city: "New York",
          sex: 'F',
          timestamp: 1270586369995573
        },
        { 
          _name: "bob", 
          city: "Jackson Heights",
          sex: 'M',
          timestamp: 1270586369995574
        }
      ] 
    }

The reason **StateUsers2** has only one timestamp is that the entire column
object is serialized as json and stored in a single column value in cassandra.

I would recommend the latter form as it has less overhead than the former. The
only thing we would gain from the **StateUsers1** form is the ability to pull
only a subset of the User attributes back in a query, but the User object stored
here in this scenario is just a cached copy of small subset of the columns of
the master User object in the Users1 column family.

Note that you can also specify fixed names at the super column level. Doing so
for **StateUsers1** would cause Active Columns to return the data in this more
collapsed form:

    {
      key: "NY",
      "alice": {
        _name: "alice", 
        city: "New York",
        sex: 'F',
        timestamps: {
          city: 1270586369995573,
          sex: 1270586369995573
        }
      },
      "bob": {
        _name: "bob", 
        city: "Jackson Heights",
        sex: 'M',
        timestamps: {
          city: 1270586369995574,
          sex: 1270586369995574
        }
      }      
    }

Do this when you have a relatively small, fixed set of super column names that
you are sure will not clash with the names of any other attributes that the
object might have. Which makes it not appropriate for this use case, as the set
of usernames would be large and arbitrary.

### "StateLastLoginUsers" column family

Using a json value type for columns to represent the 'leaf' objects, we can
support another level of querying, e.g. all the users in a given state, who have
logged in within the past x minutes (or any seconds-since-the-epoch range). An
object of this type might look like this:

    {
      key: "NY",
      columns: [
        { 
          _name: 1271184168, 
          columns: [
            {        
              _name: "alice", 
              city: "New York",
              timestamp: 1270586369995573
            },
            {        
              _name: "bob", 
              city: "Jackson Heights",
              timestamp: 1270586369995573
            },
          ]
        },
        { 
          _name: 1271184169, 
          columns: [
            {        
              _name: "chuck", 
              city: "Elmhurst",
              timestamp: 1270586369995573
            },
            {        
              _name: "dave", 
              city: "Brooklyn",
              timestamp: 1270586369995573
            },
          ]
        },
      ]
    }

We can think of what a query does as finding object nodes (the elements
represented by {..}) within this tree -- including the root -- with a subset of
that node's children. For example, if I wanted to find all the users in NY with
a birthday between x and y, I would get back a list of column objects -- the
ones with _names like "alice" and "chuck" above. If I wanted to find all the
users in NY with a birthday on a particular day, I would retrieve the super
column object -- e.g. the super column object with the _name 1271184169 above.

## Usage

### Configuration and Initialization

Active Columns depends on Cassandra-Node Bridge, so this must be set up before
you do anything.

Make sure underscore.js, cassandra-node-client.js, and active-columns.js are in
your NODE_PATH.

Include Active Columns in your program like so:

    var ActiveColumns = require('active-columns');

Then the first thing you need to do is initialize your keyspaces. This is how
keyspace for the object model examples above is initialized:

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

**cassandra\_port** and **cassandra\_host** should point to your Cassandra-Node
Bridge proxy server, not directly at your Cassandra server.

If a column family is of type "Super" it should be specified here with the
**type** property. (Active Columns will correct your error in the background if
you make a mistake regarding the column family type but you shouldn't rely on
this).

Fixed column names can be specified at the super column or column level with the
**column\_names** and **subcolumn\_names** properties.

Specify a **column\_value\_type** of "json" if you want column objects instead
of column primitives.

### Properties

#### {object}.id

Returns the identifier for the {object}. For row objects it returns the key. For
super column and column objects it returns the _name.

This property is useful when you store cached summary copies of objects as
[super] column objects in other column families and you want to reuse
presentation code to handle objects retrieved as row objects from the "master"
column family and as [super] column objects from other other column families,
and want a consistent way to refer to the "id" of the object.

#### {object}.key

Returns the key that this {object} lives under.

### Methods

#### {ActiveColumns}.get\_column\_family( keyspace, column_family )

Gets the specified column family from {ActiveColumns}.  e.g.:

    var Users1 = ActiveColumns.get_column_family("ActiveColumnsTest", "Users1");

#### {column family}.new\_object( [key,] [super\_column\_name,] [column\_name,] [init\_cols] )

Creates a new Active Columns object within {column family}.

<code>new\_object()</code> can take any number of arguments from 0 to 4. A hash
or an array in the final position will be taken as the *init\_cols*, otherwise
*init_cols* is undefined. Any string params before *init\_cols* will be taken as
the *key*, *super\_column\_name*, and *column\_name*, in that order, of the
object to be created. If none of these identifiers are provided, a column family
object with no key will be created. Active Columns will generate a UUID key for
this object when saving it.

*init\_cols* will be used to initialize the object created. it's root form --
hash or array -- must correspond to that required for the object being created.
Row objects with dynamic column names, for instance, require an array as their
*init\_cols*. a super column object with fixed subcolumn names requires a hash.

*init\_cols* can be deep if the object being created is a top-level object and
it has multiple levels of structure -- e.g.:

    var ny = StateLastLoginUsers.new_object("NY", [
      { 
        _name: 1271184169, 
        columns:[
          {_name: "dave", city: "Brooklyn", sex: "F" }, 
          {_name: "chuck", city: "Elmhurst", sex: "M" }
        ]
      },
      {
        _name: 1271184168, 
        columns:[
          {_name: "bob", city: "Jackson Heights", sex: "M" }, 
          {_name: "alice", city: "New York", sex: "F" }
        ]
      }
    ]);

This creates a row object with the key "NY", with two super columns, 1271184169
and 1271184169, each of which have column objects "dave" and "chuck", and "bob"
and "alice", respectively.

Note that I could pass the "alice" column object to some other code that later
changes some attribute and saves it. Each object in the hierachy of objects
returned by <code>new\_object()</code> has its own <code>save()</code>, etc.
methods. In the example above, "NY", 1271184169, 1271184168, "dave", "chuck",
"bob", and "alice", all have their own <code>save()</code> methods.
<code>save()</code> is recursive in a sense, though. calling <code>save()</code>
on "NY" will cause all of its descendants to be saved.

#### {column family}.find( key\_spec, [super\_column\_name,] [column\_name\_or\_predicate,] event\_listeners )

Finds objects within the {column family}.

*key_spec* is either a single key, an array of keys, or a range (e.g.
{start\_key: 'a', count: 10}). A hash in the final position will be taken as the
*event_listeners* argument. Both are mandatory.

How the second argument is interpreted depends on the type of column family. If
the column family is of type "Super" it will be taken as the
*super\_column\_name*, and an argument after that that is not the last argument
will be taken as the *column\_name\_or\_predicate*, both optional. If the column
family is not Super, a second non-ultimate argument will be taken to be the
*column\_name\_or\_predicate*, again optional.

The type of objects(s) returned depends on the the combination of column family
type, column value types, and *key\_spec*, *super\_column\_name*, and
*column\_name\_or\_predicate* provided. <code>find()</code> will return the
deepest object(s) it can based on these parameters. e.g. if the column family is
super with a json subcolumn value type, and a key, super\_column\_name, and list
of column names is provided, <code>find()</code> will return a list of column
objects; if the subcolumn value type is not json, it will return a super column
object with only the specified subcolumns selected. Specifying a list or range
of keys will do something similiar, except that results will be returned as a
hash of keys to objects in the case of a key list argument, e.g.:

    StateLastLoginUsers.find(["NY", "NJ", "CT"], 1271184168, {
      success: function(result) {
        // returns a hash of keys to super column objects
        for (var state in result) {
          result[state].columns.forEach(function(user) {
            sys.puts("User " + user._name + " in " + user.city + ", " + state +
                       " has birthday on " + result[state]._name;
          })
        }
      },
      error: function(mess) {
        sys.puts("Error looking for users with birthdays on 1271184168 in the tri-state area.")
      })

In cases where <code>find()</code> would return a single object, i.e. a single
*key* (plus other optional parameters) is specified, <code>find()</code> will
emit a **"not\_found"** event. Add a "not\_found" handler to the
*event\_listeners* hash if you wish to handle it.

#### {column\_family}.remove( key )

**Completely** removes the row object with given *key*, i.e. removes the entire
row, all columns, from Cassandra.

#### {object}.get\_super\_column\_name()

Returns the name of the super column that this {object} lives under. Only
applies to column objects under a super column.

#### {object}.save( event\_listeners, delete\_missing\_columns=true )

Saves {object}. Returns the "id" of the object as the first and only parameter
to the "success" listener (in addition to setting it on the saved object). e.g.:

    var post = Posts.new_object({ title: "A  post title.", text: "This is a post."} );
    post.save({
      success: function(result) {
        sys.puts("id of post is " + result);
        sys.puts("id of post is " + post.id);
        sys.puts("id of post is " + post.key);
      }
    })

If <code>save()</code> succeeds, all timestamps of {object} will be updated with
the new timestamp. This allows you to subsequently <code>destroy()</code> this
object if you wish.

<code>save()</code> is recursive, in a sense. If the object being saved has any
child objects they will be saved too.

If you save an object with fixed column names, any missing columns will be
deleted. If you want to prevent this, you can pass false as the last argument to
<code>save()</code>, after the event listeners hash. This is useful when you
only want to update a small number of columns for a fixed-column-name object.

#### {object}.destroy( event\_listeners )

Destroys {object}. Note: this only removes the columns represented in this
{object} from Cassandra. To completely remove *all* columns of a row object from
Cassandra use <code>{column family}.remove()</code>

## Callbacks

Since Cassandra data models tend to be highly denormalized, with cached copies
of data stored in various places, and since "indices" are decoupled from the
data they are indexing and must be updated manually when the source data
changes, it would be handy to set up some code to automatically execute whenever
you save or destroy an object. Active Columns allows you to configure various
kinds of callbacks to executed when objects are initialized, saved, found, and
destroyed.

### Sequence chaining

For each kind of callback event, a sequence (array) of functions can be
specified. Though the functions are asynchronous, and should emit "success" and
"error" events, the execution of the functions for a given callback event on a
given object will be *chained* and executed in order. If any callback in a
sequence emits an "error" event, the chain will be halted.

### Configuration

Callbacks are configured on the column family, in the
<code>callbacks</code> property of the column family. Thus, they can be
configured like so:

    ActiveColumns.initialize_keyspaces([
      { 
        name: "ActiveColumnsTest", 
        cassandra_port: 10000,
        cassandra_host: "127.0.0.1", 
        column_families: {
          Users1: { 
            column_names: ["city", "state", "last_login", "sex"],
            callbacks: {
              after_save_row: [
                function(event_listeners, previous_version) {
                  sys.puts("after_save_row for Users1 called!");
                }
              ]
            }
          }
        }
      }
    ])

or like so:

    var Users1 = get_column_family("ActiveColumnsTest", "Users1");
    Users1.add_callback("after_save_row", function(event_listeners, previous_version) {
      sys.puts("after_save_row for Users1 called!");
    };

### Note on callback recursion

Callbacks are currently not recursive. i.e, if you have callbacks defined at the
column level, they will not be executed when you new\_object/save/find/destroy
at the super column or row level. If you want that to happen you should
implement a callback at the higher level that implements some sort of iteration
over the callbacks at the lower levels. Active Columns may handle this for you
one day, but it's not a high priority.

### Events

#### after\_initialize\_{row|super_column|column} ()

There are **2** instances in which these callbacks will be executed:

* by <code>{column\_family}.new_object()</code> after an object is initialized 
* by <code>{column\_family}.find()</code>, after an object is retrieved, and
  after all *after\_find\_* callbacks have been called on the object.

The initialized object can be accessed through the <code>this</code> variable.

_**Note**: This is the one type of callback event that is executed
**synchronously**._. Thus, these callbacks should not do any I/O and their usage
should be reserved for simple things such as setting the prototype of an object,
mixing in some methods, etc.

    Users1.add_callback("after_initialize", function(event_listeners, previous_version) {
      Object.defineProperty(this, "last_login_date", {
        get function() { return convert_seconds_to_date(last_login); },
        set function(value) { last_login = convert_date_to_seconds(value); }
      })
    }

#### before\_save\_{row|super_column|column} (event\_listeners, previous\_version)

These callbacks are called before any object in the column family is saved.
*event\_listeners* is a hash containing functions to handle "success" and
"error" conditions within the callback. *previous\_version* is a copy of this
object corresponding to the previous saved version, before the the current save.
*previous\_version* corresponds to the version of this object retrieved by
<code>find()</code> if you have not saved it since retrieving it.
*previous\_version* will be <code>null</code> if this is a new object that has
not been saved before now. The saved/current version of the object can be
accessed through the <code>this</code> variable.

All callbacks in a sequence are executed in order. If any of them emit an
"error" event, the chain will be halted, and the object will not be saved to the
database. This makes *before\_save* callbacks a a good place to implement
*validations*.

    Users1.add_callback("before_save_row", function(event_listeners, previous_version) {
      
      if (!valid_state(this.state) ) {
        event_listeners.error(this.state + " is not a valid state.")
      } else
        event_listeners.success();
      }
    }

#### after\_save\_{row|super_column|column} (event\_listeners, previous\_version)

This method is called after any object in the column family is saved.
*event\_listeners* is a hash containing functions to handle "success" and
"error" conditions within the callback. *previous\_version* is a copy of this
object corresponding to the previous saved version, before the the current save.
*previous\_version* corresponds to the version of this object retrieved by
<code>find()</code> if you have not saved it since retrieving it.
*previous\_version* will be <code>null</code> if this is a new object that has
not been saved before now. The saved/current version of the object can be
accessed through the <code>this</code> variable.

    Users1.add_callback("after_save_row", function(event_listeners, previous_version) {
      // remove from StateUsers2 column family if new state is different
      if (previous_version.state != this.state) {
        StateUsers2.find(previous_version.state, previous_version._name, {
          success: function(result) {
            result.destroy({
              error: function(mess) {
                logger.error("Error destroying StateUsers2 object " + 
                                previous_version.state + "/" + previous_version._name + 
                                ":" + mess)
              }
            })
          },
          error: function(mess) {
            logger.error("Error finding StateUsers2 object " + 
                            previous_version.state + "/" + previous_version._name + 
                            ": " + mess) 
          }
        })
      }
      // update StateUsers2 with current object (this)
      var state_user = StateUsers2.new_object(this.state, {
        _name: this._name, city: this.city, sex: this.sex
      });
      state_user.save({
        error: function(mess) {
          logger.error("Error creating StateUsers2 object " + 
                          this.state + "/" + this._name + ": " + mess)           
        }
      });
      event_listeners.success();
    }


#### after\_find\_{row|super_column|column} (event\_listeners)

*after\_find\_* callbacks are called by <code>{column\_family}.find()</code>,
after an object is retrieved, and before any *after\_initialize\_* callbacks
have been called on the object. *event\_listeners* is a hash containing
functions to handle "success" and "error" conditions within the callback, the
found object can be accessed through the <code>this</code> variable.

All callbacks in a sequence are executed in order. If any of them emit an
"error" event, the chain will be halted, and an error event will be emitted by
<code>find()</code>.

If <code>find()</code> is returning a collection of objects, the sequences will
executed **in parallel** for each object. Thus it is possible that an "error"
event will be generated **multiple times** for a single <code>find()</code>
call.

_**Note**: After having implemented these callbacks, I'm finding hard to come up
with a problem that they solve that couldn't be handled by the
**after\_initialize\_** callbacks. What kind of callbacks involving asynchronous
I/O would you need to do after a <code>find()</code>? Well, anyway, I
implemented them and they're here._

#### after\_destroy\_{row|super_column|object} (event\_listeners)

This method is called after any row object in the column family is saved.
*event\_listeners* is a hash containing functions to handle "success" and
"error" conditions within the callback. The destroyed object can be accessed
through the <code>this</code> variable.

    // remove corresponding StateUsers2 object after we destroy a user object
    Users1.add_callback("after_destroy_row", function(event_listeners, previous_version) {
      StateUsers2.find(this.state, this._name, {
        success: function(result) {
          result.destroy({
            error: function(mess) {
              logger.error("Error destroying StateUsers2 object " + 
                              previous_version.state + "/" + previous_version._name + 
                              ":" + mess)
            }
          })
        },
        error: function(mess) {
          logger.error("Error finding StateUsers2 object " + 
                          previous_version.state + "/" + previous_version._name + 
                          ": " + mess) 
        }
      })
      event_listeners.success();
    }

## Future Work

### Consistency levels

Currently, consistency levels for all cassandra requests are hard-coded as ONE.
Should be able to configure the consistency level for each operation for each
kind of object in a column family.

### Associations

The idea here is that column names can point to row objects in another column
family, i.e. the StateUsers2 example. The column name acts essentially as a
foreign key. Active Columns will automatically retrieve the associated object
from the "master" column family and store summary json data in the column value,
based on the configuration. Alternatively, you could configure Active Columns to
retrieve the associated object on-the-fly. Also alternatively, you could specify
that the **value** is the the object, either a foreign key to retrieve the
object on-the-fly with, or a cached json copy of the object.

### count() method

Count all of the columns within a row object or super column object. 

### json column value type at column name level

Allowing json column value type to specified for specific column names, to allow
column objects to be mixed with column "primitives" at the same level.

## Author

Daniel Kim  
danieldkimster@gmail.com  
http://github.com/danieldkim

## License

Copyright (c) 2010 Daniel Kim

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE.
