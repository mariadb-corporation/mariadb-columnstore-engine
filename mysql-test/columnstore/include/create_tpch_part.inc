CREATE TABLE `part` (
  `P_PARTKEY` int(11) NOT NULL,
  `P_NAME` varchar(55) NOT NULL,
  `P_MFGR` char(25) NOT NULL,
  `P_BRAND` char(10) NOT NULL,
  `P_TYPE` varchar(25) NOT NULL,
  `P_SIZE` int(11) NOT NULL,
  `P_CONTAINER` char(10) NOT NULL,
  `P_RETAILPRICE` decimal(15,2) NOT NULL,
  `P_COMMENT` varchar(23) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

INSERT INTO `part` (
    `P_PARTKEY`, `P_NAME`, `P_MFGR`, `P_BRAND`, `P_TYPE`, 
    `P_SIZE`, `P_CONTAINER`, `P_RETAILPRICE`, `P_COMMENT`
) VALUES
(939, 'salmon magenta orange coral aquamarine', 'Manufacturer#1', 'Brand#14', 'PROMO BURNISHED BRASS', 41, 'WRAP PACK', 1839.93, 'ages wake doggedly of'),
(966, 'turquoise antique brown violet plum', 'Manufacturer#4', 'Brand#42', 'PROMO BURNISHED TIN', 24, 'MED CAN', 1866.96, ' the carefully'),
(1085, 'floral frosted seashell light red', 'Manufacturer#4', 'Brand#45', 'PROMO PLATED COPPER', 26, 'LG CAN', 986.08, 'anent notornis about'),
(1239, 'purple dodger lime peach burnished', 'Manufacturer#5', 'Brand#51', 'PROMO BURNISHED STEEL', 24, 'LG CASE', 1140.23, 'structions? quickly i'),
(1343, 'navy metallic green cornsilk blue', 'Manufacturer#3', 'Brand#31', 'PROMO BURNISHED TIN', 11, 'WRAP PKG', 1244.34, 'cuses'),
(2988, 'medium white firebrick cream sky', 'Manufacturer#3', 'Brand#33', 'PROMO POLISHED COPPER', 3, 'MED DRUM', 1890.98, '. boldly pendin'),
(3024, 'lemon burnished goldenrod khaki saddle', 'Manufacturer#1', 'Brand#15', 'PROMO BURNISHED COPPER', 10, 'MED PACK', 927.02, ', eve'),
(3126, 'chiffon azure orange seashell blanched', 'Manufacturer#3', 'Brand#34', 'PROMO POLISHED COPPER', 1, 'LG DRUM', 1029.12, 'special dolphins al'),
(3142, 'papaya dim beige salmon pink', 'Manufacturer#3', 'Brand#33', 'PROMO BRUSHED STEEL', 50, 'SM CAN', 1045.14, 'quests. slyly'),
(3357, 'violet dim metallic lemon spring', 'Manufacturer#3', 'Brand#32', 'PROMO ANODIZED COPPER', 40, 'SM CASE', 1260.35, 'lent pl'),
(3625, 'indian peach coral brown linen', 'Manufacturer#5', 'Brand#54', 'PROMO PLATED TIN', 50, 'MED CASE', 1528.62, 'ccounts cajole ca'),
(4119, 'blanched salmon gainsboro grey coral', 'Manufacturer#4', 'Brand#43', 'PROMO BRUSHED BRASS', 40, 'JUMBO PACK', 1023.11, 'dependencies wa'),
(4466, 'black white chocolate blue drab', 'Manufacturer#2', 'Brand#21', 'PROMO ANODIZED NICKEL', 22, 'SM CAN', 1370.46, 'r the regular braids:'),
(88035, 'cornsilk navajo cornflower metallic midnight', 'Manufacturer#4', 'Brand#45', 'PROMO BRUSHED NICKEL', 16, 'JUMBO DRUM', 1023.03, 'ly even theodolites a'),
(88362, 'ivory dodger thistle royal olive', 'Manufacturer#2', 'Brand#21', 'ECONOMY POLISHED STEEL', 39, 'WRAP BAG', 1350.36, 'even requests.'),
(123927, 'firebrick powder light chiffon violet', 'Manufacturer#1', 'Brand#14', 'MEDIUM PLATED TIN', 41, 'WRAP CAN', 1950.92, 'ackages. blithely'),
(128449, 'cream linen salmon smoke blue', 'Manufacturer#2', 'Brand#25', 'STANDARD BURNISHED BRASS', 10, 'WRAP BOX', 1477.44, 'nal, regular inst'),
(137469, 'steel royal frosted peru sienna', 'Manufacturer#4', 'Brand#42', 'ECONOMY BURNISHED COPPER', 17, 'LG PKG', 1506.46, 'unts aft'),
(155190, 'chocolate lace cornflower rosy light', 'Manufacturer#4', 'Brand#44', 'PROMO BRUSHED NICKEL', 9, 'JUMBO JAR', 1245.19, 'sly unusual orbits use'),
(182052, 'blue floral drab yellow dark', 'Manufacturer#5', 'Brand#52', 'MEDIUM PLATED COPPER', 38, 'JUMBO PKG', 1134.05, 'ic, regular foxes. bo'),
(197921, 'goldenrod yellow royal salmon linen', 'Manufacturer#5', 'Brand#53', 'MEDIUM BRUSHED BRASS', 10, 'WRAP DRUM', 2018.92, 'r the quickl');