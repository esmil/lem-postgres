lem-postgres
============


About
-----

lem-postgres is a [PostgreSQL][] library for the [Lua Event Machine][lem].
It allows you to query PostgreSQL databases without blocking
other coroutines.

[lem]: https://github.com/esmil/lem
[postgresql]: http://www.postgresql.org/

Installation
------------

Get the source and do

    make
    make install

This installs the library under `/usr/local/lib/lua/5.1/`.
Use

    make PREFIX=<your custom path> install

to install to `<your custom path>/lib/lua/5.1/`.


Usage
-----

Import the module using something like

    local postgres = require 'lem.postgres'

This sets `postgres` to a table with a single function.

* __postgres.connect(conninfo)__

  Connect to the database given by parameters in the `conninfo` string.
  Returns a new database connection object on success,
  or otherwise `nil` followed by an error message.

The metatable of database connection objects can be found under
__postgres.Connection__.

Database connection objects has the following methods.

* __db:close()__

  Close the database connection.

  Returns `true` on success or otherwise `nil, 'already closed'`,
  if the connection was already closed.

* __db:reset()__

  Reset the database connection.

  Returns `true` on success or otherwise `nil` followed by an error message.
  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.
  If another coroutine closes this connection while the reset is going on
  the error message be `'interrupted'`.

* __db:exec(query, ...)__

  Execute an SQL query. If the query string is parametized
  (using $1, $2 etc.) the parameters must be given after the
  query string.

  If the query is a SELECT the result will be returned in a Lua table
  with entries 1, 2, ... for each row.
  Each row is again a Lua table with entries 1, 2, ... for each
  returned column.

  Returns `true` or the result of the SELECT statement on success,
  or otherwise `nil` followed by an error message.
  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.
  If another coroutine closes this connection while the reset is going on
  the error message be `'interrupted'`.

* __db:prepare(name, query)__

  Creates a prepared SQL statement under the given name.

  Returns `true` on success or otherwise `nil` followed by an error message.
  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.
  If another coroutine closes this connection while the reset is going on
  the error message be `'interrupted'`.

* __db:run(name, ...)__

  Runs the prepared statement given by the name. If the statement
  is parameterized (using $1, $2 etc.) the parameters must be given
  after the name of the statement.

  If the prepared statement is a SELECT the result will be returned in
  a Lua table with entries 1, 2, ... for each row.
  Each row is again a Lua table with entries 1, 2, ... for each
  returned column.

  Returns `true` or the result of the SELECT statement on success,
  or otherwise `nil` followed by an error message.
  If another coroutine is using this database connection the error message
  will be `'busy'`.
  If the connection is closed the error message will be `'closed'`.
  If another coroutine closes this connection while the reset is going on
  the error message be `'interrupted'`.

* __db:put(line)__

  This method should be called after a `COPY <table> FROM STDIN`-query.
  Call it once for each row in the table.

  Returns `true` on success or otherwise `nil` followed by an error message.

* __db:done([error])__

  When all rows of a `COPY <table> FROM STDIN`-query have been sent to the
  server, call this method to signal end of transmission.

  If an error message is supplied this will signal an error to the server.

  Returns `true` on success or otherwise `nil` followed by an error message.
  Calling this method with an error argument will also cause it to return
  `nil` followed by an error message.

* __db:get()__

  Call this method to receive rows after a `COPY <table> TO STDIN`-query.

  Returns one complete line as a Lua string or `true` to signal end of
  transmission.
  On error `nil` followed by an error message will be returned.


License
-------

lem-postgres is free software. It is distributed under the terms of the
[GNU General Public License][gpl].

[gpl]: http://www.fsf.org/licensing/licenses/gpl.html


Contact
-------

Please send bug reports, patches, feature requests, praise and general gossip
to me, Emil Renner Berthing <esmil@mailme.dk>.
