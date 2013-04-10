About
-----

The sphinx(full text search server) client package for the Go programming language.

## Installation

`go get github.com/yunge/gosphinx`


## Testing

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

## Examples
```Go
import (
  "github.com/yunge/gosphinx"
)

sc := NewSphinxClient().Server(host, port).Query(words, index, "Some comment...")
if err := sc.Error(); err != nil {
	return fmt.Errorf("Init sphinx client> %v", err)
}

for _, match := range sc.Matches {
	// handle match.DocId
}

```
More examples can be found in test files.

## LICENSE

BSD License
[http://opensource.org/licenses/bsd-license](http://opensource.org/licenses/bsd-license)
