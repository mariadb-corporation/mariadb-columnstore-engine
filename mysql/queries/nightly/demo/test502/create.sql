drop table if exists orders2;

CREATE TABLE `orders2` (
  `o_orderkey` int(11) DEFAULT NULL,
  `o_custkey` int(11) DEFAULT NULL,
  `o_orderstatus` char(1) DEFAULT NULL,
  `o_totalprice` decimal(12,2) DEFAULT NULL,
  `o_orderdate` date DEFAULT NULL,
  `o_orderpriority` char(15) DEFAULT NULL,
  `o_clerk` char(15) DEFAULT NULL,
  `o_shippriority` int(11) DEFAULT NULL,
  `o_comment` varchar(79) DEFAULT NULL
) ENGINE=InfiniDB;
