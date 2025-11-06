# create tpch tables in innodb with indexes for queryaccelerator testing
#foreign keys are added in indices.inc after data is loaded into tables
--disable_result_log

create table region (
        r_regionkey int PRIMARY KEY,
        r_name char (25),
        r_comment varchar (152)
) ENGINE=InnoDB;

create table nation (
        n_nationkey int PRIMARY KEY,
        n_name char (25),
        n_regionkey int,
        n_comment varchar (152)
) ENGINE=InnoDB;

create table supplier (
        s_suppkey int PRIMARY KEY,
        s_name char (25),
        s_address varchar (40),
        s_nationkey int,
        s_phone char (15),
        s_acctbal decimal(15,2),
        s_comment varchar (101)
) ENGINE=InnoDB;

create table customer (
        c_custkey int PRIMARY KEY,
        c_name varchar (25),
        c_address varchar (40),
        c_nationkey int,
        c_phone char (15),
        c_acctbal decimal(15,2),
        c_mktsegment char (10),
        c_comment varchar (117)
) ENGINE=InnoDB;

create table part (
        p_partkey int PRIMARY KEY,
        p_name varchar (55),
        p_mfgr char (25),
        p_brand char (10),
        p_type varchar (25),
        p_size int,
        p_container char (10),
        p_retailprice decimal(15,2),
        p_comment varchar (23)
) ENGINE=InnoDB;

create table partsupp (
        ps_partkey int,
        ps_suppkey int,
        ps_availqty int,
        ps_supplycost decimal(15,2),
        ps_comment varchar (199),
        PRIMARY KEY (ps_partkey, ps_suppkey)
) ENGINE=InnoDB;

create table orders (
        o_orderkey int PRIMARY KEY,
        o_custkey int,
        o_orderstatus char (1),
        o_totalprice decimal(15,2),
        o_orderdate date,
        o_orderpriority char (15),
        o_clerk char (15),
        o_shippriority int,
        o_comment varchar (79)
) ENGINE=InnoDB;

create table lineitem (
        l_orderkey int,
        l_partkey int,
        l_suppkey int,
        l_linenumber int,
        l_quantity decimal(15,2),
        l_extendedprice decimal(15,2),
        l_discount decimal(15,2),
        l_tax decimal(15,2),
        l_returnflag char (1),
        l_linestatus char (1),
        l_shipdate date,
        l_commitdate date,
        l_receiptdate date,
        l_shipinstruct char (25),
        l_shipmode char (10),
        l_comment varchar (44),
        PRIMARY KEY (l_orderkey, l_linenumber)
) ENGINE=InnoDB;

--enable_result_log
#
