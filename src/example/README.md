# Example

## Setup
To set up the keyspace, run
```
cqlsh -f ./schema.cql
```

To load the data, run
```
dsbulk load -k test -t itest -header false -url ./itest.csv
```

## Run
To run rainier, run
```
java -jar ../../target/rainier-0.1-SNAPSHOT-jar-with-dependencies.jar -host localhost -f /tmp/cmds.cql -argfile "pkey_in:/tmp/pkey.csv,ccol_in:/tmp/ccol.csv" -numIterations 100 -numThreads 10
```

This will run a series of `SELECT` and `INSERT` statements.  Before the run, the `Z` column is empty
for all rows.  The `INSERT` statement will set the `Z` column.  You can check that rows were touched
by running
```
cqlsh -e "SELECT * FROM test.itest WHERE z >= 0 ALLOW FILTERING"
```

You can reset the data by running
```
dsbulk unload -k test -t itest -header false | grep -v ",$" | awk -F, '{printf("UPDATE test.itest SET z = null WHERE pkey = %s AND ccol = %s;", $1, $2);}' | cqlsh
```

