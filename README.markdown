About
-----

The sphinx(full text search server) client package for the Go programming language.

Installation
------------

`go get github.com/yunge/gosphinx`


Testing
-------

Import "documents.sql" to "test" database in mysql;

Change the mysql password in sphinx.conf;

Copy the test.xml to default dir in sphinx.conf:
`cp test.xml /usr/local/sphinx/var/data`

Index the test data:
`indexer -c /gosphinx_path/sphinx.conf --all --rotate`

Start sphinx searchd with "sphinx.conf":
`searchd -c /gosphinx_path/sphinx.conf`

Then "cd" to gosphinx:

`go test`


Differs from other languages's lib
----------------------------------

No GetLastError()

Go can return multi values, it's unnecessary to set a "error" field, gosphinx just return error as another return values.

But GetLastWarning() is still remained, and still has IsConnectError() to "Checks whether the last error was a network error on API side".


## LICENSE

BSD License
[http://opensource.org/licenses/bsd-license](http://opensource.org/licenses/bsd-license)
