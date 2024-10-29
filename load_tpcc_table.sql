COPY tpcc_table(db_key, field0, field1, field2, field3, field4)
FROM '/home/tpcc_table.csv'
DELIMITER ','
CSV;

-- SELECT * FROM tpcc_table;