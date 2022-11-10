-- sudo setcap cap_sys_ptrace=eip /usr/bin/gdb

drop table lineitem_bulk;

create table lineitem_bulk (
I_orderkey BIGINT NOT NULL,
I_partkey BIGINT NOT NULL,
I_suppkey INTEGER NOT NULL,
I_linenumber INTEGER NOT NULL,
I_quantity DECIMAL (15, 2) NOT NULL,
I_extendedprice DECIMAL (15, 2) NOT NULL,
I_discount DECIMAL (15, 2) NOT NULL,
I_tax DECIMAL (12, 2) NOT NULL,
I_returnflag char (1) DEFAULT NULL,
I_linestatus char (1) DEFAULT NULL,
I_shipdate date NOT NULL,
I_commitdate date DEFAULT NULL,
I_receiptdate date DEFAULT NULL,
I_shipinstruct char (25) DEFAULT NULL,
I_shipmode char (10) DEFAULT NULL,
I_comment varchar (44) DEFAULT NULL,
primary key (I_orderkey,
I_linenumber));

SET GLOBAL secure_file_priv = "";
set global ob_query_timeout=60000000;
load data infile "/root/1m.csv" into table lineitem_bulk fields terminated by "|";