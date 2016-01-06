select crc32(r_name), crc32(r_regionkey) from region;
select crc32(cvchar8), crc32(cdate) from datatypetestm where crc32(cchar5) > 2550494110;
select crc32(r_name), count(*) from region group by 1 order by 1;
select * from region order by crc32(r_comment);
select crc32(c2) from sub1;
