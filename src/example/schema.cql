CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE test.itest (pkey int, ccol int, x int, y int, z int, PRIMARY KEY (pkey, ccol)); 
