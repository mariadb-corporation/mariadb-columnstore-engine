select concat_ws(":", r_regionkey, r_name, r_comment) from region order by 1;
select concat_ws(s1,c1,c2,c3) from sub1 where concat_ws(c1,c2,c3)>300;
select concat_ws(" ", cidx, cchar5, cvchar5), count(*) from datatypetestm group by 1 order by 1;
select cidx from datatypetestm where concat_ws(" ", cchar5, cvchar5) is not null;
select concat_ws(r_comment, r_comment,r_comment,r_comment) from region;
select concat_ws('-',cidx,null,null,0) from datatypetestm;
select cchar4, cchar5, concat_ws(null, cchar4, cchar5) from datatypetestm;
