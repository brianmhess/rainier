# Rainier
The Cascades

## Description
Rainier will run a list of commands that cascade.  The output from previous statements
will be used as the input to the next statements.

We use the `<col> AS <ref>` to name the fields in the `SELECT` statements.  We use named
bindings to fill in the blanks.

###An example
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

###But there's more
This is only kind of interesting.  But it's a good building block.  Rainier will allow you to 
specify initial values on the command-line, too.  For example, we could tweak the previous
example with the list of statements as:
```
SELECT x AS x_1 FROM ks.tbl1 WHERE pkey = :pkey_in AND ccol = :ccol_in;
SELECT y FROM ks.tbl2 WHERE pkey = :x_1;
```
Then we can supply those values on the command line using the `-args` command. 
Specifically, here we would supply `-args "pkey_in:1,ccol_in:2"`.

###And even more
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

###And a little more
There are situations where you would like to run a set of queries a few times in a row
(which can mimic real-world behavior in some scenarios).  We can do that with the `-minRepeat`
and `-maxRepeat` parameters.  That will choose a random value in `[minRepeat,maxRepeat]` and
simply repeat the same chain of queries with the same arguments each time.

We will want to control the number of iterations.  We can do that via `-numIterations`.
