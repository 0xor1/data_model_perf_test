data_model_perf_test
====================

A simple set of performance tests for comparing the speed of querying tree data structures from various databases. Currently
the test implementations have only been written for RDBMs using MySql and graph databases using Neo4j. If you'd like to add
more implementations, a wider range of tests or more data schemas simply follow the pattern of the existing tests.

##Prerequisites

These are the latest versions known to work with the tests

+ Go 1.7.1
+ MySql 5.7 (community edition)
+ Neo4j 3.0.4 (community edition)

The configuration of the tests is in the main function in main/main.go, the current configuration assumes you have:

+ Neo4j database running and accessible at `"bolt://neo4j:root@localhost:7687"`
+ MySql server running with a blank database `dmpt` and accessible at `"root:root@tcp(localhost:3306)/dmpt?multiStatements=true"`

Update as necessary.

##Data

All tests are run on perfect K-ary trees where each node has two integer properties `id` and `value` which
are both initialised to be the same value, the nodes are assigned incremental ids in breadth-first order starting
from 0 at the root of the tree. This makes it easy to perform certain validation steps on the query results to ensure they
are working as intended.

##Run

```
go get github.com/robsix/data_model_perf_test
cd $GOPATH/github.com/robsix/data_model_perf_test
go build
./main.exe
```