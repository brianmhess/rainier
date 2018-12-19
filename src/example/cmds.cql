SELECT x AS x_1, y AS y_1 FROM test.itest WHERE pkey = :pkey_in AND ccol = :ccol_in;
SELECT pkey AS pkey_2, ccol AS ccol_2 FROM test.itest WHERE pkey = :x_1 AND ccol = :y_1;
SELECT x AS x_3, y AS y_3 FROM test.itest WHERE pkey = :pkey_2 AND ccol = :ccol_2;
SELECT pkey AS pkey_4, ccol AS ccol_4 FROM test.itest WHERE pkey = :x_3 AND ccol = :y_3;
INSERT INTO test.itest(pkey,ccol,z) VALUES (:pkey_4, :ccol_4, :pkey_4);
