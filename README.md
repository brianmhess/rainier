# Rainier
The Cascades

## Description
Rainier will run a list of commands that cascade.  The output from previous statements
will be used as the input to the next statements.

We use the `<col> AS <ref>` to name the fields in the `SELECT` statements.  We use named
bindings to fill in the blanks.

### An example
If we wanted to chain the following two CQL statements together:
```
SELECT x FROM ks.tbl1 WHERE pkey = 1 AND ccol = 2;
SELECT y FROM ks.tbl2 WHERE pkey = <X from the previous query>;
```
we can do that by supplying a list of queries as follows:
```
SELECT x AS x_1 FROM ks.tbl1 WHERE pkey = 1 AND ccol = 2;
SELECT y FROM ks.tbl2 WHERE pkey = :x_1;
```

### But there's more
This is only kind of interesting.  But it's a good building block.  Rainier will allow you to 
specify initial values on the command-line, too.  For example, we could tweak the previous
example with the list of statements as:
```
SELECT x AS x_1 FROM ks.tbl1 WHERE pkey = :pkey_in AND ccol = :ccol_in;
SELECT y FROM ks.tbl2 WHERE pkey = :x_1;
```
Then we can supply those values on the command line using the `-args` command. 
Specifically, here we would supply `-args "pkey_in:1,ccol_in:2"`.

### And even more
Okay, but sometimes I want to do many runs and provide randomized inputs.  For this we can
supply a file with one value per line, and tell Rainier to choose a random line from the file 
as the input.

For example, suppose we have a file called `/tmp/pkey.csv` that has the following contents:
``` 
1
2
3
```
And we have another file called `/tmp/ccol.csv` that has the following contents:
``` 
2
3
4
5
6
```
We could tell Rainier to choose a random `pkey` value from the `/tmp/pkey.csv` file and a
random `ccol` value from the `/tmp/ccol.csv` file.  We do this via the `-argfile` parameter.
For example, `-argfile "pkey_in:/tmp/pkey.csv,ccol_in:/tmp/ccol.csv"`.

### And a little more
There are situations where you would like to run a set of queries a few times in a row
(which can mimic real-world behavior in some scenarios).  We can do that with the `-minRepeat`
and `-maxRepeat` parameters.  That will choose a random value in `[minRepeat,maxRepeat]` and
simply repeat the same chain of queries with the same arguments each time.

We will want to control the number of iterations.  We can do that via `-numIterations`.

### Just a little more
You can run this with multiple threads.  If you set `-numThreads`, a threadpool with that
many threads will be created.  Each "task" is one set of parameters (essentially, one random
seed value).  Each task will run some number of repeats of the chain as specified by `minRepeat`
and `maxRepeat`.  There will be a total of `numIterations` tasks, but with repeats may have
more than `numIteration` chains run altogether.

## Usage
``` 
$ java -jar target/rainier-0.1-SNAPSHOT-jar-with-dependencies.jar <arguments>
version: 0.0.1
Usage: rainier -host <hostname> -f <input file>
OPTIONS:
  -host <hostname>               Contact point for DSE [required]
  -f <input file>                File of queries to run
  -configFile <filename>         File with configuration options [none]
  -port <portNumber>             CQL Port Number [9042]
  -user <username>               Cassandra username [none]
  -pw <password>                 Password for user [none]
  -ssl-truststore-path <path>    Path to SSL truststore [none]
  -ssl-truststore-pw <pwd>       Password for SSL truststore [none]
  -ssl-keystore-path <path>      Path to SSL keystore [none]
  -ssl-keystore-pw <pwd>         Password for SSL keystore [none]
  -numThreads <numThreads>       How many parallel queries to run [1]
  -consistencyLevel <CL>         Consistency Level [LOCAL_ONE]
  -arg <key:val,...>             List of key:value pairs of arguments [none]
  -argfile <arg:argfilename,...> List of argument file names [none]
  -numIterations <num>           Number of iterations to run [1000]
  -minRepeat <min>               Minimum number of times to repeat a run [1]
  -maxRepeat <max>               Maximum number of times to repeat a run [1]
  -rate <tps>                    Query rate in transactions/sec [50000]
```